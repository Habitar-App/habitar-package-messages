import { Channel, Connection, ConsumeMessage } from "amqplib"
import { Logger } from "winston"
import { IMessagingConsumerAdapter } from "../contracts/IMessagingConsumer"
import { IMessagingPublisherAdapter } from "../contracts/IMessagingPublisher"

type RabbitConsumerAdapterOptions = {
  exchange: string
  queue: string
  successMessage: string
}

export class RabbitConsumerAdapter implements IMessagingConsumerAdapter {
  private useCase: { execute: (data: any) => Promise<any> | any }
  private options: RabbitConsumerAdapterOptions
  private messagingPublisher: IMessagingPublisherAdapter
  private logger: Logger
  private amqpConnection: (data: { exchange: string, queues: string[] }) => Promise<{ channel: Channel, connection: Connection }>
  private config?: Partial<{ requeue: boolean }>

  constructor({ useCase, messagingPublisher, logger, amqpConnection, config = {} }: {
    useCase: { execute: (data: any) => Promise<any> | any },
    messagingPublisher: IMessagingPublisherAdapter,
    logger: Logger,
    amqpConnection: (data: { exchange: string, queues: string[] }) => Promise<{ channel: Channel, connection: Connection }>,
    config?: Partial<{ requeue: boolean }>
  }, options: RabbitConsumerAdapterOptions) {
    this.useCase = useCase
    this.options = options
    this.messagingPublisher = messagingPublisher
    this.logger = logger
    this.amqpConnection = amqpConnection
    this.config = { ...config, requeue: config?.requeue || false }

    this.consume()
  }

  protected async consume() {
    const { queue, exchange, successMessage } = this.options
    const queueName = `${process.env.SERVICE_NAME}.${queue}`

    const { channel } = await this.amqpConnection({ exchange, queues: [queueName] })

    await channel.consume(queueName, async (message) => {
      const queueMessage = message?.content?.toString()
      if (!queueMessage) return

      const jsonQueueMessage: any = JSON.parse(queueMessage)

      if (Array.isArray(jsonQueueMessage)) {
        await this.consumeArray(channel, jsonQueueMessage, message, queueName, successMessage)
      } else {
        await this.consumeObject(channel, jsonQueueMessage, message, queueName, successMessage)
      }
    })
  }

  private async consumeArray(channel: Channel, jsonArray: any[], message: ConsumeMessage | null, queueName: string, successMessage: string) {
    for (const item of jsonArray) {
      await this.consumeObject(channel, item, message, queueName, successMessage)
    }
  }

  private async consumeObject(channel: Channel, jsonObject: any, message: ConsumeMessage | null, queueName: string, successMessage: string) {
    try {
      if (jsonObject.messageOrigin === process.env.SERVICE_NAME)
        return channel.ack(message as ConsumeMessage)

      await this.useCase.execute(jsonObject)

      channel.ack(message as ConsumeMessage)

      if (jsonObject?.requeueUid)
        await this.messagingPublisher.sendMessage({
          exchange: 'success.messages',
          message: { requeueUid: jsonObject?.requeueUid, }
        })

      this.logger.info(`${successMessage}`, JSON.stringify(jsonObject))
    } catch (error: any) {
      channel.nack(message as ConsumeMessage, undefined, this.config?.requeue)

        await this.messagingPublisher.sendMessage({
          exchange: 'error.messages',
          message: {
            requeueUid: jsonObject?.requeueUid,
            origin: process.env.SERVICE_NAME,
            queue: queueName,
            data: jsonObject,
            error: { message: error.message, stack: error.stack }
          }
        })

        this.logger.error(`Fail when consuming message from rabbitmq queue.`, {
          exchange: this.options.exchange,
          message: JSON.stringify(jsonObject),
          queue: queueName,
          error: JSON.stringify(error?.message),
        })
    }
  }
}
