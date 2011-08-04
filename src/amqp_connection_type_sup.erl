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
-module(amqp_connection_type_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/0, start_direct/1, start_network/3,
         start_heartbeat_fun/1]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    {ok, _} = supervisor2:start_link(?MODULE, []).

start_direct(Sup) ->
    {ok, Collector} =
        supervisor2:start_child(
          Sup,
          {collector, {rabbit_queue_collector, start_link, []},
           transient, ?MAX_WAIT, worker, [rabbit_queue_collector]}),
    {ok, Collector}.

start_network(Sup, Sock, Connection) ->
    {ok, AState} = rabbit_command_assembler:init(?PROTOCOL),
    {ok, Writer} =
        supervisor2:start_child(
          Sup,
          {writer, {rabbit_writer, start_link,
                    [Sock, 0, ?FRAME_MIN_SIZE, ?PROTOCOL, Connection]},
           transient, ?MAX_WAIT, worker, [rabbit_writer]}),
    {ok, MainReader} =
        supervisor2:start_child(
          Sup,
          {main_reader, {amqp_main_reader, start_link,
                         [Sock, Connection, AState]},
           transient, ?MAX_WAIT, worker, [amqp_main_reader]}),
    {ok, MainReader, AState, Writer}.

start_heartbeat_fun(Sup) ->
    rabbit_heartbeat:start_heartbeat_fun(Sup).

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
