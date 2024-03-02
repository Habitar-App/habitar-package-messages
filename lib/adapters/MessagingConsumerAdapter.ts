import { Channel, ConsumeMessage } from "amqplib"
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
  private channel: Channel

  constructor({ useCase, messagingPublisher, logger, channel }: {
    useCase: { execute: (data: any) => Promise<any> | any },
    messagingPublisher: IMessagingPublisherAdapter,
    logger: Logger,
    channel: Channel
  }, options: RabbitConsumerAdapterOptions) {
    this.useCase = useCase
    this.options = options
    this.messagingPublisher = messagingPublisher
    this.logger = logger
    this.channel = channel

    this.consume()
  }

  async consume() {
    const { queue, exchange, successMessage } = this.options
    const queueName = `${process.env.SERVICE_NAME}.${queue}`

    await this.channel.consume(queueName, async (message) => {
      const queueMessage = message?.content?.toString()
      if (!queueMessage) return

      const jsonQueueMessage: any = JSON.parse(queueMessage)
      try {
        if (jsonQueueMessage.messageOrigin === process.env.SERVICE_NAME)
          return this.channel.ack(message as ConsumeMessage)

        await this.useCase.execute(jsonQueueMessage)
        this.channel.ack(message as ConsumeMessage)
        this.logger.info(`${successMessage}`, queueMessage)
      } catch (error: any) {
        this.channel.nack(message as ConsumeMessage, undefined, false)
        await this.messagingPublisher.sendMessage({
          exchange: 'error.messages',
          message: {
            requeueUid: jsonQueueMessage?.requeueUid,
            origin: process.env.SERVICE_NAME,
            queue: queueName,
            data: jsonQueueMessage,
            error: { message: error.message, stack: error.stack }
          }
        })
        this.logger.error(`Fail when consuming message from rabbitmq queue.`, {
          exchange: exchange,
          message: queueMessage,
          queue: queueName,
          error: JSON.stringify(error?.message),
        })
      }
    })
  }
}
