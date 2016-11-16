#
# Copyright (C) 2016-2016, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

require 'ygg/agent/base'

require 'securerandom'
require 'time'

require 'pg'

require 'meter_recorder/version'
require 'meter_recorder/task'

module MeterRecorder

class App < Ygg::Agent::Base
  self.app_name = 'meter_recorder'
  self.app_version = VERSION
  self.task_class = Task

  def prepare_default_config
    app_config_files << File.join(File.dirname(__FILE__), '..', 'config', 'meter_recorder.conf')
    app_config_files << '/etc/yggdra/meter_recorder.conf'
  end

  def agent_boot
    @pg = PG::Connection.open(mycfg.db.to_h)

    @ins_statement = @pg.prepare('insert_measure',
      'INSERT INTO acao_meter_measures (meter_id, at, voltage, current, power, app_power, rea_power, ' +
                                       'frequency, power_factor, exported_energy, imported_energy, total_energy) ' +
      'VALUES ($1,now(),$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)')

    @lookup_statement = @pg.prepare('select_meter', 'SELECT * FROM acao_meters WHERE uuid=$1')

    @amqp.ask(AM::AMQP::MsgExchangeDeclare.new(
      channel_id: @amqp_chan,
      name: mycfg.exchange,
      type: :topic,
      durable: true,
      auto_delete: false,
    )).value

    @amqp.ask(AM::AMQP::MsgQueueDeclare.new(
      channel_id: @amqp_chan,
      name: mycfg.queue,
      durable: true,
      auto_delete: false,
      arguments: {
        :'x-message-ttl' => (3 * 86400 * 1000),
      },
    )).value

    @amqp.ask(AM::AMQP::MsgQueueBind.new(
      channel_id: @amqp_chan,
      queue_name: mycfg.queue,
      exchange_name: mycfg.exchange,
      routing_key: '#'
    )).value

    @msg_consumer = @amqp.ask(AM::AMQP::MsgConsume.new(
      channel_id: @amqp_chan,
      queue_name: mycfg.queue,
      send_to: self.actor_ref,
    )).value.consumer_tag

    @meters = {}
  end

  def meter_update(message)
    payload = JSON.parse(message.payload).deep_symbolize_keys!

    meter = @meters[message.routing_key]
    if !meter
      res = @pg.exec_prepared('select_meter', [ message.routing_key ])
      if res.ntuples == 0
        log.warn "Meter #{message.routing_key} not found, ignoring message"
        return
      end

      meter = @meters[message.routing_key] = res[0]['id']
    end

    @pg.exec_prepared('insert_measure', [
      meter,
      payload[:voltage],
      payload[:current],
      payload[:power],
      payload[:app_power],
      payload[:rea_power],
      payload[:frequency],
      payload[:power_factor],
      payload[:exported_energy],
      payload[:imported_energy],
      payload[:total_energy],
    ])
  end


  def actor_handle(message)
    case message
    when AM::AMQP::MsgDelivery

      if message.consumer_tag == @msg_consumer
        case message.headers[:type]
        when 'METER_UPDATE'
          meter_update(message)

          @amqp.tell AM::AMQP::MsgAck.new(channel_id: @amqp_chan, delivery_tag: message.delivery_tag)

        else
          @amqp.tell AM::AMQP::MsgAck.new(channel_id: @amqp_chan, delivery_tag: message.delivery_tag)
        end
      else
        super
      end
    else
      super
    end
  end

end
end
