type RabbitConsumerAdapterOptions = {
  exchange: string
  queue: string
  successMessage: string
}

export interface IMessagingConsumerAdapter {
  consume(useCase: { execute: (data: any) => any }, option: RabbitConsumerAdapterOptions): Promise<void>
}
