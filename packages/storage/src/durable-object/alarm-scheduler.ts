export class AlarmScheduler {
  constructor(private readonly storage: DurableObjectStorage) {}

  async schedule(at: number): Promise<void> {
    await this.storage.setAlarm(at);
  }

  async cancel(): Promise<void> {
    await this.storage.deleteAlarm();
  }
}
