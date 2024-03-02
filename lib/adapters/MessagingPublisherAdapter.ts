import { connect } from "amqplib";
import { Logger } from "winston";
import { IMessagingPublisherAdapter } from "../contracts/IMessagingPublisher";

export class RabbitPublisherAdapter implements IMessagingPublisherAdapter {
  private logger: Logger
  constructor({ logger }: { logger: Logger }) {
    this.logger = logger
  }

  async sendMessage({ exchange, message }: { exchange: string; message: any }) {
    message = { ...message, messageOrigin: process.env.SERVICE_NAME }
    const connection = await connect({
      heartbeat: 5,
      hostname: process.env.AMQP_CONNECTION,
      username: process.env.AMQP_USER,
      password: process.env.AMQP_PASS,
    })
    const channel = await connection.createChannel()

    await channel.assertExchange(exchange, "fanout")

    this.logger.info(`+ New message on exchange "${exchange}"`)

    channel.publish(exchange, "", Buffer.from(JSON.stringify(message)))

    await channel.close()
    await connection.close()
  }
}
