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
-module(amqp_writer_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/0, start_writer/4]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    {ok, _} = supervisor2:start_link(?MODULE, []).

start_writer(Sup, Sock, ChNumber, Channel) ->
    {ok, _} = supervisor2:start_child(Sup, [Sock, ChNumber, ?FRAME_MIN_SIZE,
                                            ?PROTOCOL, Channel]).

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{writer, {rabbit_writer, start_link, []},
            intrinsic, ?MAX_WAIT, worker, [rabbit_writer]}]}}.
