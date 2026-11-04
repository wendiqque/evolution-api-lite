import { PrismaRepository } from '@api/repository/repository.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { configService, Log, Rabbitmq } from '@config/env.config';
import { Logger } from '@config/logger.config';
import * as amqp from 'amqplib/callback_api';

import { EmitData, EventController, EventControllerInterface } from '../event.controller';

// A dropped connection (a proxy cutting an idle link, a broker restart) is replaced, retrying with
// a growing wait, so events are not lost until the next restart
const RECONNECT_DELAY_MS = { min: 1000, max: 30 * 1000 };

export class RabbitmqController extends EventController implements EventControllerInterface {
  public amqpChannel: amqp.Channel | null = null;
  private amqpConnection: amqp.Connection | null = null;
  private reconnectDelay = RECONNECT_DELAY_MS.min;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private droppedEvents = 0;
  private readonly logger = new Logger('RabbitmqController');

  constructor(prismaRepository: PrismaRepository, waMonitor: WAMonitoringService) {
    super(prismaRepository, waMonitor, configService.get<Rabbitmq>('RABBITMQ')?.ENABLED, 'rabbitmq');
  }

  public async init(): Promise<void> {
    if (!this.status) {
      return;
    }

    await this.connect();
  }

  // Opens a connection and channel and watches both: when either closes, they are dropped and a
  // new connection is opened after a wait
  private async connect(): Promise<void> {
    try {
      const connected = await new Promise<boolean>((resolve) => {
        const uri = configService.get<Rabbitmq>('RABBITMQ').URI;
        const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;

        amqp.connect(uri, (error, connection) => {
          if (error) {
            this.logger.error('AMQP connection error: ' + error.message);
            resolve(false);
            return;
          }

          connection.on('error', (err) => this.logger.error('AMQP connection error: ' + err?.message));
          connection.on('close', () => {
            if (this.amqpConnection === connection) this.dropConnection('connection closed');
          });

          connection.createChannel((channelError, channel) => {
            if (channelError) {
              this.logger.error('AMQP channel error: ' + channelError.message);
              connection.close(() => undefined);
              resolve(false);
              return;
            }

            channel.on('error', (err) => this.logger.error('AMQP channel error: ' + err?.message));
            channel.on('close', () => {
              if (this.amqpChannel === channel) this.dropConnection('channel closed');
            });

            channel.assertExchange(rabbitmqExchangeName, 'topic', {
              durable: true,
              autoDelete: false,
            });

            this.amqpConnection = connection;
            this.amqpChannel = channel;
            this.reconnectDelay = RECONNECT_DELAY_MS.min;

            if (this.droppedEvents > 0) {
              this.logger.warn(`AMQP reconnected; ${this.droppedEvents} events could not be sent while it was down`);
              this.droppedEvents = 0;
            }

            this.logger.info('AMQP initialized');

            resolve(true);
          });
        });
      });

      if (!connected) {
        this.scheduleReconnect();
        return;
      }

      if (configService.get<Rabbitmq>('RABBITMQ')?.GLOBAL_ENABLED) this.initGlobalQueues();
    } catch (error) {
      this.logger.error('AMQP initialization caught error: ' + error?.message);
      this.scheduleReconnect();
    }
  }

  private dropConnection(reason: string): void {
    if (!this.amqpConnection && !this.amqpChannel) return;

    this.logger.warn(`AMQP ${reason}; reconnecting`);
    const connection = this.amqpConnection;
    this.amqpChannel = null;
    this.amqpConnection = null;
    try {
      connection?.close(() => undefined);
    } catch {
      // Already closed
    }
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;

    const wait = this.reconnectDelay;
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, RECONNECT_DELAY_MS.max);
    this.logger.warn(`AMQP reconnecting in ${wait / 1000}s`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, wait);
  }

  private set channel(channel: amqp.Channel) {
    this.amqpChannel = channel;
  }

  public get channel(): amqp.Channel {
    return this.amqpChannel;
  }

  public async emit({
    instanceName,
    origin,
    event,
    data,
    serverUrl,
    dateTime,
    sender,
    apiKey,
    integration,
  }: EmitData): Promise<void> {
    if (integration && !integration.includes('rabbitmq')) {
      return;
    }

    if (!this.status) {
      return;
    }

    const instanceRabbitmq = await this.get(instanceName);
    const rabbitmqLocal = instanceRabbitmq?.events;
    const rabbitmqGlobal = configService.get<Rabbitmq>('RABBITMQ').GLOBAL_ENABLED;
    const rabbitmqEvents = configService.get<Rabbitmq>('RABBITMQ').EVENTS;
    const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;
    const we = event.replace(/[.-]/gm, '_').toUpperCase();
    const logEnabled = configService.get<Log>('LOG').LEVEL.includes('WEBHOOKS');

    const message = {
      event,
      instance: instanceName,
      data,
      server_url: serverUrl,
      date_time: dateTime,
      sender,
      apikey: apiKey,
    };

    if (instanceRabbitmq?.enabled && !this.amqpChannel && Array.isArray(rabbitmqLocal) && rabbitmqLocal.includes(we)) {
      if (this.droppedEvents === 0)
        this.logger.warn(`AMQP is not connected; ${event} for ${instanceName} was not sent`);
      this.droppedEvents++;
    }

    if (instanceRabbitmq?.enabled && this.amqpChannel) {
      if (Array.isArray(rabbitmqLocal) && rabbitmqLocal.includes(we)) {
        const exchangeName = instanceName ?? rabbitmqExchangeName;

        let retry = 0;

        while (retry < 3) {
          try {
            await this.amqpChannel.assertExchange(exchangeName, 'topic', {
              durable: true,
              autoDelete: false,
            });

            const eventName = event.replace(/_/g, '.').toLowerCase();

            const queueName = `${instanceName}.${eventName}`;

            await this.amqpChannel.assertQueue(queueName, {
              durable: true,
              autoDelete: false,
              arguments: {
                'x-queue-type': 'quorum',
              },
            });

            await this.amqpChannel.bindQueue(queueName, exchangeName, eventName);

            await this.amqpChannel.publish(exchangeName, event, Buffer.from(JSON.stringify(message)));

            if (logEnabled) {
              const logData = {
                local: `${origin}.sendData-RabbitMQ`,
                ...message,
              };

              this.logger.log(logData);
            }

            break;
          } catch (error) {
            retry++;
          }
        }
      }
    }

    if (rabbitmqGlobal && rabbitmqEvents[we] && this.amqpChannel) {
      const exchangeName = rabbitmqExchangeName;

      let retry = 0;

      while (retry < 3) {
        try {
          await this.amqpChannel.assertExchange(exchangeName, 'topic', {
            durable: true,
            autoDelete: false,
          });

          const queueName = event;

          await this.amqpChannel.assertQueue(queueName, {
            durable: true,
            autoDelete: false,
            arguments: {
              'x-queue-type': 'quorum',
            },
          });

          await this.amqpChannel.bindQueue(queueName, exchangeName, event);

          await this.amqpChannel.publish(exchangeName, event, Buffer.from(JSON.stringify(message)));

          if (logEnabled) {
            const logData = {
              local: `${origin}.sendData-RabbitMQ-Global`,
              ...message,
            };

            this.logger.log(logData);
          }

          break;
        } catch (error) {
          retry++;
        }
      }
    }
  }

  private async initGlobalQueues(): Promise<void> {
    this.logger.info('Initializing global queues');

    const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;
    const events = configService.get<Rabbitmq>('RABBITMQ').EVENTS;

    if (!events) {
      this.logger.warn('No events to initialize on AMQP');

      return;
    }

    const eventKeys = Object.keys(events);

    eventKeys.forEach((event) => {
      if (events[event] === false) return;

      const queueName = `${event.replace(/_/g, '.').toLowerCase()}`;
      const exchangeName = rabbitmqExchangeName;

      this.amqpChannel.assertExchange(exchangeName, 'topic', {
        durable: true,
        autoDelete: false,
      });

      this.amqpChannel.assertQueue(queueName, {
        durable: true,
        autoDelete: false,
        arguments: {
          'x-queue-type': 'quorum',
        },
      });

      this.amqpChannel.bindQueue(queueName, exchangeName, event);
    });
  }
}
