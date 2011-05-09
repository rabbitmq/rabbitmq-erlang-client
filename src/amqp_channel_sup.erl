%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

%% @private
-module(amqp_channel_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/4]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(AmqpParams, Connection, InfraArgs, ChNumber) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    {ok, ChPid} = supervisor2:start_child(
                    Sup, {channel, {amqp_channel, start_link,
                                    [AmqpParams, Connection, ChNumber,
                                     start_writer_fun(Sup, AmqpParams,
                                                      InfraArgs, ChNumber)]},
                          intrinsic, brutal_kill, worker, [amqp_channel]}),
    {ok, AState} = init_command_assembler(AmqpParams),
    {ok, Sup, {ChPid, AState}}.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

start_writer_fun(_Sup, #amqp_params_direct{},
                 [ConnectionPid, Node, User, VHost, Collector], ChNumber) ->
    fun () ->
            {ok, RabbitCh} =
                rpc:call(Node, rabbit_direct, start_channel,
                         [ChNumber, self(), ConnectionPid, ?PROTOCOL, User,
                          VHost, ?CLIENT_CAPABILITIES, Collector]),
            link(RabbitCh),
            {ok, RabbitCh}
    end;
start_writer_fun(Sup, #amqp_params_network{}, [Sock], ChNumber) ->
    fun () ->
            {ok, _} = supervisor2:start_child(
                        Sup,
                        {writer, {rabbit_writer, start_link,
                                  [Sock, ChNumber, ?FRAME_MIN_SIZE, ?PROTOCOL,
                                   self()]},
                         transient, ?MAX_WAIT, worker, [rabbit_writer]})
    end.

init_command_assembler(#amqp_params_direct{}) ->
    {ok, none};
init_command_assembler(#amqp_params_network{}) ->
    rabbit_command_assembler:init(?PROTOCOL).

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
