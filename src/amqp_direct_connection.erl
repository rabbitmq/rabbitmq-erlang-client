%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.

%% @private
-module(amqp_direct_connection).

-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start_link/3, connect/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).
-export([emit_stats/1]).

-record(state, {sup,
                params = #amqp_params{},
                adapter_info = none,
                collector,
                channels_manager,
                closing = false,
                server_properties,
                stats_timer,
                start_infrastructure_fun}).

-record(closing, {reason,
                  close,
                  from  = none}).

-define(CREATION_EVENT_KEYS, [pid, protocol, address, port, peer_address, peer_port,
                              user, vhost, client_properties]).
-define(STATISTICS_KEYS, [pid, channels, state]).
-define(INFO_KEYS,
        (amqp_connection:info_keys() ++ ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid])).

%%---------------------------------------------------------------------------
%% Internal interface
%%---------------------------------------------------------------------------

start_link(AmqpParams, SIF, AdapterInfo) ->
    gen_server:start_link(?MODULE,
                          [self(), AmqpParams, SIF, AdapterInfo], []).

connect(Pid) ->
    gen_server:call(Pid, connect, infinity).

emit_stats(Pid) ->
    gen_server:cast(Pid, emit_stats).

ensure_stats_timer(State = #state{stats_timer = StatsTimer,
                                  closing = false}) ->
    Self = self(),
    State#state{stats_timer = rabbit_event:ensure_stats_timer(
                                StatsTimer,
                                fun() -> emit_stats(Self) end)};
ensure_stats_timer(State) ->
    State.

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

init([Sup, AmqpParams, SIF, AdapterInfo]) ->
    {ok, #state{sup                      = Sup,
                adapter_info             = AdapterInfo,
                params                   = AmqpParams,
                stats_timer              = rabbit_event:init_stats_timer(),
                start_infrastructure_fun = SIF}}.

handle_call({command, Command}, From, #state{closing = Closing} = State) ->
    case Closing of
        false ->
            State1 = ensure_stats_timer(State),
            handle_command(Command, From, State1);
        _ ->
            {reply, closing, State}
    end;
handle_call({info, Items}, _From, State) ->
    {reply, infos(Items, State), State};
handle_call(info_keys, _From, State) ->
    {reply, ?INFO_KEYS, State};
handle_call(connect, _From, State) ->
    {reply, ok, do_connect(State)}.

handle_cast(emit_stats, State) ->
    {noreply, internal_emit_stats(State)};
handle_cast(Cast, State) ->
    {stop, {unexpected_cast, Cast}, State}.

handle_info({hard_error_in_channel, Pid, Reason}, State) ->
    ?LOG_WARN("Connection (~p) closing: channel (~p) received hard error ~p "
              "from server~n", [self(), Pid, Reason]),
    {stop, Reason, State};
handle_info({send_hard_error, AmqpError}, State) ->
    {noreply, send_error(AmqpError, State)};
handle_info({channel_internal_error, _Pid, _Reason}, State) ->
    {noreply, send_error(#amqp_error{name = internal_error}, State)};
handle_info(all_channels_terminated, State) ->
    handle_all_channels_terminated(State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

%%---------------------------------------------------------------------------
%% Command handling
%%---------------------------------------------------------------------------

handle_command({open_channel, ProposedNumber}, _From, State =
                   #state{collector        = Collector,
                          params           = #amqp_params{virtual_host = VHost,
                                                          username = User},
                          channels_manager = ChMgr}) ->
    {reply, amqp_channels_manager:open_channel(ChMgr, ProposedNumber,
                                               [User, VHost, Collector]),
     State};

handle_command({close, Close}, From, State) ->
    {noreply, set_closing_state(flush, #closing{reason = app_initiated_close,
                                                close  = Close,
                                                from   = From},
                                State)}.

%%---------------------------------------------------------------------------
%% Infos
%%---------------------------------------------------------------------------

infos(Items, State) ->
    [{Item, i(Item, State)} || Item <- Items].

i(server_properties, State) -> State#state.server_properties;
i(is_closing,        State) -> State#state.closing =/= false;
i(amqp_params,       State) -> State#state.params;
i(channels,          State) -> amqp_channels_manager:num_channels(
                                 State#state.channels_manager);
i(pid,              _State) -> self();
i(state, #state{closing = false}) -> running;
i(state, _)                       -> closing;
%% AMQP Params
i(user,
  #state{params = Params}) -> Params#amqp_params.username;
i(vhost,
  #state{params = Params}) -> Params#amqp_params.virtual_host;
i(client_properties,
  #state{params = Params}) -> Params#amqp_params.client_properties;
%% Optional adapter info
i(protocol, #state{adapter_info = #adapter_info{protocol = unknown}}) ->
    protocol_info(?PROTOCOL);
i(protocol, #state{adapter_info = AdapterInfo}) ->
    protocol_info(AdapterInfo#adapter_info.protocol);
i(address, #state{adapter_info = AdapterInfo}) ->
    AdapterInfo#adapter_info.address;
i(port, #state{adapter_info = AdapterInfo}) ->
    AdapterInfo#adapter_info.port;
i(peer_address, #state{adapter_info = AdapterInfo}) ->
    AdapterInfo#adapter_info.peer_address;
i(peer_port, #state{adapter_info = AdapterInfo}) ->
    AdapterInfo#adapter_info.peer_port;

i(Item, _State) -> throw({bad_argument, Item}).

protocol_info(?PROTOCOL) ->
    ?PROTOCOL:version();
protocol_info(Protocol = {_Family, _Version}) ->
    Protocol.

%%---------------------------------------------------------------------------
%% Closing
%%---------------------------------------------------------------------------

%% Changes connection's state to closing.
%%
%% ChannelCloseType can be flush or abrupt
%%
%% The precedence of the closing MainReason's is as follows:
%%     app_initiated_close, error, server_initiated_close
%% (i.e.: a given reason can override the currently set one if it is later
%% mentioned in the above list). We can rely on erlang's comparison of atoms
%% for this.
set_closing_state(ChannelCloseType, Closing, State = #state{closing = false}) ->
    NewState = State#state{closing = Closing},
    signal_connection_closing(ChannelCloseType, NewState),
    NewState;
%% Already closing, override situation
set_closing_state(ChannelCloseType, NewClosing,
                  State = #state{closing = CurClosing}) ->
    ResClosing =
        if
            %% Override (rely on erlang's comparison of atoms)
            NewClosing#closing.reason >= CurClosing#closing.reason ->
                NewClosing;
            %% Do not override
            true ->
                CurClosing
        end,
    NewState = State#state{closing = ResClosing},
    %% Do not override reason in channels (because it might cause channels to
    %% to exit with different reasons) but do cause them to close abruptly
    %% if the new closing type requires it
    case ChannelCloseType of
        abrupt -> signal_connection_closing(abrupt, NewState);
        _      -> ok
    end,
    NewState.

signal_connection_closing(ChannelCloseType, #state{channels_manager = ChMgr,
                                                   closing = Closing}) ->
    amqp_channels_manager:signal_connection_closing(ChMgr, ChannelCloseType,
                                                    closing_to_reason(Closing)).

handle_all_channels_terminated(State = #state{closing = Closing,
                                              collector = Collector}) ->
    #state{closing = #closing{}} = State, % assertion
    rabbit_queue_collector:delete_all(Collector),
    case Closing#closing.from of none -> ok;
                                 From -> gen_server:reply(From, ok)
    end,
    rabbit_event:notify(connection_closed, [{pid, self()}]),
    {stop, closing_to_reason(Closing), State}.

closing_to_reason(#closing{close = #'connection.close'{reply_code = 200}}) ->
    normal;
closing_to_reason(#closing{reason = Reason,
                           close  = #'connection.close'{reply_code = Code,
                                                        reply_text = Text}}) ->
    {Reason, Code, Text}.

send_error(#amqp_error{} = AmqpError, State) ->
    {true, 0, Close} =
        rabbit_binary_generator:map_exception(0, AmqpError, ?PROTOCOL),
    set_closing_state(abrupt, #closing{reason = error, close = Close}, State).

%%---------------------------------------------------------------------------
%% Connecting to the broker
%%---------------------------------------------------------------------------

do_connect(State0 = #state{params = #amqp_params{username = User,
                                                 password = Pass,
                                                 virtual_host = VHost},
                          stats_timer = StatsTimer}) ->
    case lists:keymember(rabbit, 1, application:which_applications()) of
        true  -> ok;
        false -> exit(broker_not_found_in_vm)
    end,
    rabbit_access_control:user_pass_login(User, Pass),
    rabbit_access_control:check_vhost_access(
            #user{username = User, password = Pass}, VHost),
    State1 = start_infrastructure(State0),
    rabbit_event:notify(connection_created,
                        infos(?CREATION_EVENT_KEYS, State1)),
    rabbit_event:if_enabled(StatsTimer,
                            fun() -> internal_emit_stats(State1) end),
    State2 = ensure_stats_timer(State1),
    State2#state{server_properties = rabbit_reader:server_properties()}.

start_infrastructure(State = #state{start_infrastructure_fun = SIF}) ->
    {ok, {ChMgr, Collector}} = SIF(),
    State#state{channels_manager = ChMgr, collector = Collector}.

internal_emit_stats(State = #state{stats_timer = StatsTimer}) ->
    io:format("~w", [infos(?STATISTICS_KEYS, State)]),
    rabbit_event:notify(connection_stats, infos(?STATISTICS_KEYS, State)),
    State#state{stats_timer = rabbit_event:reset_stats_timer(StatsTimer)}.
