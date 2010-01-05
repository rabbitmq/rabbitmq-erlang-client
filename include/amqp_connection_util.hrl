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
%%

%% Generic connection state (to be used with the amqp_connection_util module)
-record(gen_c_state, {channels,
                      closing,
                      all_channels_closed_event_handler,
                      all_channels_closed_event_params}).


%% Macros to call amqp_connection_util functions that require a #gen_c_state{}.
%% The second version is for functions that return a #gen_c_state{} - converts
%% it back to #dc_state{} or #nc_state{}.
%% These macros require gen_c_state and from_gen_c_state functions defined in
%% the current module.
-define(UTIL(Func, Params, State),
        apply(amqp_connection_util, Func, Params ++ [gen_c_state(State)])).
-define(UTIL2(Func, Params, State),
        from_gen_c_state(?UTIL(Func, Params, State), State)).
