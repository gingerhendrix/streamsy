/**
 * The example's agent: one small tool, one short prompt, one provider chosen by
 * environment. Fold owns everything about how this runs — the loop, the tool
 * settlement, the projections, the resume semantics. Nothing here knows that the
 * durable log happens to be a Streamsy stream.
 */
import {
  anthropicModel,
  defineAgent,
  defineTool,
  openaiModel,
  type AgentDefinition,
  type FoldModel,
} from "@humanlayer/fold-core";
import { Config, Effect, Layer, Option, Schema } from "effect";
import { FetchHttpClient, type HttpClient } from "effect/http";
import { scriptedModel, textTurn, toolCallTurn } from "./scripted-model.ts";

export const SYSTEM_PROMPT = [
  "You are the Streamsy Fold example agent.",
  "Answer briefly. When the user asks about text, use the text_stats tool rather than counting yourself.",
].join(" ");

/**
 * A harmless, deterministic tool. It exists to prove that a tool call and its
 * result become durable Fold entries in the Streamsy stream, so keep it free of
 * side effects.
 */
export const textStatsTool = defineTool({
  name: "text_stats",
  description: "Return the character count, word count, and upper-cased form of a piece of text.",
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.Struct({
    characters: Schema.Finite,
    words: Schema.Finite,
    shout: Schema.String,
  }),
  handler: ({ text }) =>
    Effect.succeed({
      characters: text.length,
      words: text.trim() === "" ? 0 : text.trim().split(/\s+/).length,
      shout: text.toUpperCase(),
    }),
});

/** The example agent, over whichever model the host resolved. */
export const exampleAgent = (model: FoldModel): AgentDefinition =>
  defineAgent({
    name: "streamsy-fold-example",
    model,
    systemPrompt: SYSTEM_PROMPT,
    tools: [textStatsTool],
  });

export class MissingCredentialsError extends Schema.TaggedError<MissingCredentialsError>()(
  "MissingCredentialsError",
  { message: Schema.String },
) {}

const missingCredentials = () =>
  new MissingCredentialsError({
    message:
      "No provider credentials found. Set OPENAI_API_KEY (optionally FOLD_AGENT_MODEL) or ANTHROPIC_API_KEY.",
  });

const ScriptCommands = Schema.Array(
  Schema.Union([
    Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
    Schema.Struct({
      type: Schema.Literal("tool"),
      id: Schema.String,
      name: Schema.String,
      params: Schema.Json,
    }),
  ]),
);

const setting = (name: string) =>
  Config.String(name).pipe(Config.option, Config.map(Option.filter((value) => value !== "")));

/** Provider settings. An empty variable counts as unset. */
const ModelSettings = Config.all({
  script: setting("FOLD_AGENT_SCRIPT"),
  openaiKey: setting("OPENAI_API_KEY"),
  anthropicKey: setting("ANTHROPIC_API_KEY"),
  model: setting("FOLD_AGENT_MODEL"),
});

const scriptedFromJson = (fixture: string) =>
  Schema.decodeEffect(Schema.fromJsonString(ScriptCommands))(fixture).pipe(
    Effect.mapError(
      (cause) =>
        new MissingCredentialsError({
          message: `Invalid FOLD_AGENT_SCRIPT fixture: ${String(cause)}`,
        }),
    ),
    Effect.flatMap((commands) =>
      scriptedModel(
        commands.map((command) =>
          command.type === "text"
            ? textTurn(command.text)
            : toolCallTurn(command.id, command.name, command.params),
        ),
      ),
    ),
    Effect.map(({ model }) => model),
  );

/** Fold's built-in providers need an HttpClient. The example reaches them over fetch. */
const overFetch = (model: FoldModel<HttpClient.HttpClient>): FoldModel => ({
  ...model,
  make: Effect.gen(function* () {
    const services = yield* Layer.build(FetchHttpClient.layer);

    return yield* model.make.pipe(Effect.provideContext(services));
  }),
});

/**
 * Pick a model from config. A scripted fixture wins, then OpenAI, then
 * Anthropic. Tests that do not run the CLI supply a scripted model instead.
 */
export const configuredModel: Effect.Effect<
  FoldModel,
  MissingCredentialsError | Config.ConfigError
> = Effect.gen(function* () {
  const settings = yield* ModelSettings;

  if (Option.isSome(settings.script)) return yield* scriptedFromJson(settings.script.value);

  if (Option.isSome(settings.openaiKey))
    return overFetch(
      openaiModel({
        apiKey: settings.openaiKey.value,
        model: Option.getOrElse(settings.model, () => "gpt-5.6"),
      }),
    );

  if (Option.isSome(settings.anthropicKey))
    return overFetch(
      anthropicModel({
        apiKey: settings.anthropicKey.value,
        ...Option.match(settings.model, { onNone: () => ({}), onSome: (model) => ({ model }) }),
      }),
    );

  return yield* missingCredentials();
});
