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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

%% @private
-module(amqp_connection_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/1]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(AmqpParams) ->
    {Type, Module} =
        case AmqpParams of
            #amqp_params_direct{}  -> {direct,  amqp_direct_connection};
            #amqp_params_network{} -> {network, amqp_network_connection}
        end,
    case Module:socket(AmqpParams) of
        {ok, Sock} ->
            start_link(AmqpParams, Type, Module, Sock);
        Err ->
            Err
    end.

start_link(AmqpParams, Type, Module, Sock) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    {ok, Connection} = supervisor2:start_child(
                         Sup,
                         {connection, {amqp_gen_connection, start_link,
                                       [Module, AmqpParams, []]},
                          intrinsic, brutal_kill, worker,
                          [amqp_gen_connection]}),
    {ok, ChSupSup} = supervisor2:start_child(
                       Sup,
                       {channel_sup_sup, {amqp_channel_sup_sup, start_link,
                                          [Type, Connection]},
                        intrinsic, infinity, supervisor,
                        [amqp_channel_sup_sup]}),
    {ok, ChMgr} = supervisor2:start_child(
                    Sup,
                    {channels_manager, {amqp_channels_manager, start_link,
                                        [Connection, ChSupSup]},
                     transient, ?MAX_WAIT, worker, [amqp_channels_manager]}),
    Extra = case Type of
                direct ->
                    {ok, _CTSup, Collector} =
                        supervisor2:start_child(
                          Sup,
                          {connection_type_sup, {amqp_connection_type_sup,
                                                 start_link_direct, []},
                           intrinsic, infinity, supervisor,
                           [amqp_connection_type_sup]}),
                    Collector;
                network ->
                    {ok, CTSup, {_MainReader, _AState, Writer}} =
                        supervisor2:start_child(
                          Sup,
                          {connection_type_sup, {amqp_connection_type_sup,
                                                 start_link_network,
                                                 [Sock, Connection, ChMgr]},
                           transient, infinity, supervisor,
                           [amqp_connection_type_sup]}),
                    {Sock, Writer, CTSup}
            end,
    case amqp_gen_connection:connect(Connection, ChMgr, Extra) of
        ok  -> {ok, Sup, Connection};
        Err -> Err
    end.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
