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
-module(amqp_connection_util).

-include("amqp_client.hrl").
-include("amqp_connection_util.hrl").

-export([handle_exit/3]).
-export([check_trigger_all_channels_closed_event/1, unregister_channel/2]).

%%---------------------------------------------------------------------------
%% Generic handling of exits
%%---------------------------------------------------------------------------

handle_exit(Pid, Reason, #gen_c_state{closing = Closing,
                                      channels = Channels}) ->
    case amqp_channel_util:is_channel_pid_registered(Pid, Channels) of
        true  -> handle_channel_exit(Pid, Reason, Closing);
        false -> ?LOG_WARN("Connection (~p) closing: received unexpected "
                           "exit signal from (~p). Reason: ~p~n",
                           [self(), Pid, Reason]),
                 other
    end.

handle_channel_exit(_Pid, normal, _Closing) ->
    %% Normal amqp_channel shutdown
    normal;
handle_channel_exit(Pid, {server_initiated_close, Code, _Text}, false) ->
    %% Channel terminating (server sent 'channel.close')
    {IsHardError, _, _} = rabbit_framing:lookup_amqp_exception(
                            rabbit_framing:amqp_exception(Code)),
    case IsHardError of
        true  -> ?LOG_WARN("Connection (~p) closing: channel (~p) "
                           "received hard error from server~n", [self(), Pid]),
                 stop;
        false -> normal
    end;
handle_channel_exit(_Pid, {_CloseReason, _Code, _Text}, Closing)
        when Closing =/= false ->
    %% Channel terminating due to connection closing
    normal;
handle_channel_exit(Pid, Reason, _Closing) ->
    %% amqp_channel dies with internal reason - this takes
    %% the entire connection down
    ?LOG_WARN("Connection (~p) closing: channel (~p) died. Reason: ~p~n",
              [self(), Pid, Reason]),
    close.

%%---------------------------------------------------------------------------
%% Other connection utilities
%%---------------------------------------------------------------------------

check_trigger_all_channels_closed_event(#gen_c_state{closing = false} = State) ->
    State;
check_trigger_all_channels_closed_event(
        #gen_c_state{channels = Channels,
                     closing = Closing,
                     all_channels_closed_event_handler = Handler,
                     all_channels_closed_event_params = Params} = State) ->
    NewClosing =
        case amqp_channel_util:is_channel_dict_empty(Channels) of
            true  -> Handler(Params, Closing);
            false -> Closing
        end,
    State#gen_c_state{closing = NewClosing}.

unregister_channel(Pid, #gen_c_state{channels = Channels} = State) ->
    NewChannels = amqp_channel_util:unregister_channel_pid(Pid, Channels),
    NewState = State#gen_c_state{channels = NewChannels},
    check_trigger_all_channels_closed_event(NewState).
