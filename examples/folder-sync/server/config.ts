import { Config, Option } from "effect";

export const ServerConfig = Config.all({
  port: Config.Port("PORT").pipe(Config.withDefault(1339)),
  databasePath: Config.option(Config.String("FOLDER_SYNC_DB")).pipe(
    Config.map(Option.filter((path) => path !== "")),
  ),
});
