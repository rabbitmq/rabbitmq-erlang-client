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

-module(lib_amqp).

-include_lib("rabbit.hrl").
-include_lib("rabbit_framing.hrl").
-include("amqp_client.hrl").

-compile(export_all).

%% --------------------------------------------------------------------------
%% Connection

start_connection() ->
    amqp_connection:start_direct("guest", "guest").

start_connection(Host) ->
    start_connection(Host, ?PROTOCOL_PORT).

start_connection(Host, Port) when is_number(Port) ->
    start_connection("guest", "guest", Host, Port);

start_connection(Username, Password) ->
    amqp_connection:start(Username, Password).

start_connection(Username, Password, Host) ->
    amqp_connection:start(Username, Password, Host).

start_connection(Username, Password, Host, Port) ->
    amqp_connection:start_network(Username, Password, Host, Port).


close_connection(Connection) ->
    ConnectionClose = #'connection.close'{reply_code = 200,
                                          reply_text = <<"Goodbye">>,
                                          class_id = 0,
                                          method_id = 0},
    #'connection.close_ok'{} = amqp_connection:close(Connection,
                                                     ConnectionClose),
    ok.


%% --------------------------------------------------------------------------
%% Channel

start_channel(Connection) ->
    amqp_connection:open_channel(Connection).


close_channel(Channel) ->
    ChannelClose = #'channel.close'{reply_code = 200,
                                    reply_text = <<"Goodbye">>,
                                    class_id = 0,
                                    method_id = 0},
    #'channel.close_ok'{} = amqp_channel:call(Channel, ChannelClose),
    ok.


teardown(Connection, Channel) ->
    close_channel(Channel),
    close_connection(Connection).


%% --------------------------------------------------------------------------
%% Exchange

declare_exchange(Channel, ExchangeDeclare = #'exchange.declare'{}) ->
    amqp_channel:call(Channel, ExchangeDeclare);

declare_exchange(Channel, X) ->
    declare_exchange(Channel, X, <<"direct">>).

declare_exchange(Channel, X, Type) ->
    ExchangeDeclare = #'exchange.declare'{exchange = X,
                                          type = Type},
    declare_exchange(Channel, ExchangeDeclare).


delete_exchange(Channel, ExchangeDelete = #'exchange.delete'{}) ->
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, ExchangeDelete);

delete_exchange(Channel, X) ->
    ExchangeDelete = #'exchange.delete'{exchange = X},
    delete_exchange(Channel, ExchangeDelete).


%%---------------------------------------------------------------------------
%% Queue

declare_queue(Channel) ->
    declare_queue(Channel, <<>>).

declare_queue(Channel, QueueDeclare = #'queue.declare'{}) ->
    #'queue.declare_ok'{queue = QueueName}
        = amqp_channel:call(Channel, QueueDeclare),
    QueueName;

declare_queue(Channel, Q) ->
    QueueDeclare = #'queue.declare'{queue = Q},
    declare_queue(Channel, QueueDeclare).


declare_private_queue(Channel) ->
    declare_queue(Channel, #'queue.declare'{exclusive = true,
                                            auto_delete = true}).

declare_private_queue(Channel, QueueName) ->
    declare_queue(Channel, #'queue.declare'{queue = QueueName,
                                            exclusive = true,
                                            auto_delete = true}).


delete_queue(Channel, QueueDelete = #'queue.delete'{}) ->
    #'queue.delete_ok'{} = amqp_channel:call(Channel, QueueDelete);

delete_queue(Channel, Q) ->
    QueueDelete = #'queue.delete'{queue = Q},
    delete_queue(Channel, QueueDelete).


bind_queue(Channel, X, Q, Binding) ->
    QueueBind = #'queue.bind'{queue = Q, exchange = X,
                              routing_key = Binding},
    bind_queue(Channel, QueueBind).

bind_queue(Channel, QueueBind = #'queue.bind'{}) ->
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).


unbind_queue(Channel, X, Q, Binding) ->
    QueueUnbind = #'queue.unbind'{queue = Q, exchange = X,
                             routing_key = Binding, arguments = []},
    unbind_queue(Channel, QueueUnbind).

unbind_queue(Channel, QueueUnbind = #'queue.unbind'{}) ->
    #'queue.unbind_ok'{} = amqp_channel:call(Channel, QueueUnbind).


%%---------------------------------------------------------------------------
%% Publish

% TODO This whole section of optional properties and mandatory flags may have
% to be re-thought

publish(Channel, BasicPublish = #'basic.publish'{}, Content = #content{}) ->
    publish_internal(fun amqp_channel:call/3, Channel, BasicPublish, Content).

publish(Channel, X, RoutingKey, Payload) ->
    publish(Channel, X, RoutingKey, Payload, false).

publish(Channel, X, RoutingKey, Payload, Mandatory)
        when is_boolean(Mandatory)->
    publish(Channel, X, RoutingKey, Payload, Mandatory,
            amqp_util:basic_properties());

publish(Channel, X, RoutingKey, Payload, Properties) ->
    publish(Channel, X, RoutingKey, Payload, false, Properties).

publish(Channel, X, RoutingKey, Payload, Mandatory, Properties) ->
    publish_internal(fun amqp_channel:call/3,
                     Channel, X, RoutingKey, Payload, Mandatory, Properties).

async_publish(Channel, BasicPublish = #'basic.publish'{},
              Content = #content{}) ->
    publish_internal(fun amqp_channel:cast/3, Channel, BasicPublish, Content).

async_publish(Channel, X, RoutingKey, Payload) ->
    async_publish(Channel, X, RoutingKey, Payload, false).

async_publish(Channel, X, RoutingKey, Payload, Mandatory) ->
    publish_internal(fun amqp_channel:cast/3, Channel, X, RoutingKey,
                      Payload, Mandatory, amqp_util:basic_properties()).

publish_internal(Fun, Channel, BasicPublish = #'basic.publish'{},
                 Content=#content{}) ->
    Fun(Channel, BasicPublish, Content).

publish_internal(Fun, Channel, X, RoutingKey,
                 Payload, Mandatory, Properties) ->
    BasicPublish = #'basic.publish'{exchange = X,
                                    routing_key = RoutingKey,
                                    mandatory = Mandatory},
    {ClassId, _MethodId} = rabbit_framing:method_id('basic.publish'),
    Content = #content{class_id = ClassId,
                       properties = Properties,
                       properties_bin = none,
                       payload_fragments_rev = [Payload]},
    publish_internal(Fun, Channel, BasicPublish, Content).


%%---------------------------------------------------------------------------
%% Get, subscribe, ack

get(Channel, BasicGet = #'basic.get'{no_ack = NoAck}) ->
    {Method, Content} = amqp_channel:call(Channel, BasicGet),
    case Method of
        'basic.get_empty' -> 'basic.get_empty';
        _ ->
            #'basic.get_ok'{delivery_tag = DeliveryTag} = Method,
            case NoAck of
                true -> Content;
                false -> {DeliveryTag, Content}
            end
    end;

get(Channel, Q) ->
    get(Channel, Q, true).

get(Channel, Q, NoAck) ->
    BasicGet = #'basic.get'{queue = Q, no_ack = NoAck},
    get(Channel, BasicGet).


subscribe(Channel, Consumer, BasicConsume = #'basic.consume'{}) ->
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:subscribe(Channel, BasicConsume, Consumer),
    ConsumerTag;

subscribe(Channel, Q, Consumer) ->
    subscribe(Channel, Q, Consumer, <<>>, true).

subscribe(Channel, Q, Consumer, NoAck) when is_boolean(NoAck) ->
    subscribe(Channel, Q, Consumer, <<>>, NoAck);

subscribe(Channel, Q, Consumer, Tag) ->
    subscribe(Channel, Q, Consumer, Tag, true).

subscribe(Channel, Q, Consumer, Tag, NoAck) ->
    BasicConsume = #'basic.consume'{queue = Q,
                                    consumer_tag = Tag,
                                    no_ack = NoAck},
    subscribe(Channel, Consumer, BasicConsume).


unsubscribe(Channel, BasicCancel = #'basic.cancel'{}) ->
    #'basic.cancel_ok'{} = amqp_channel:call(Channel, BasicCancel),
    ok;

unsubscribe(Channel, Tag) ->
    BasicCancel = #'basic.cancel'{consumer_tag = Tag},
    unsubscribe(Channel, BasicCancel).


ack(Channel, BasicAck = #'basic.ack'{}) ->
    ok = amqp_channel:cast(Channel, BasicAck);

ack(Channel, DeliveryTag) ->
    BasicAck = #'basic.ack'{delivery_tag = DeliveryTag, multiple = false},
    ack(Channel, BasicAck).


%%---------------------------------------------------------------------------
%% QoS

set_prefetch_count(Channel, Prefetch) ->
    BasicQos = #'basic.qos'{prefetch_count = Prefetch},
    qos(Channel, BasicQos).

qos(Channel, BasicQos = #'basic.qos'{}) ->
    amqp_channel:call(Channel, BasicQos).
