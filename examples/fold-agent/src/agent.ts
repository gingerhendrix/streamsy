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
import { Effect, Schema } from "effect";

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

/**
 * Pick a live provider from the environment. OpenAI is the documented first
 * path; Anthropic is accepted when only that key is present. Tests never reach
 * this — they supply a scripted model instead.
 */
export const modelFromEnv = (
  env: Record<string, string | undefined>,
): Effect.Effect<FoldModel, MissingCredentialsError> =>
  Effect.suspend(() => {
    const openaiKey = env["OPENAI_API_KEY"];
    if (openaiKey !== undefined && openaiKey !== "") {
      return Effect.succeed(
        openaiModel({ apiKey: openaiKey, model: env["FOLD_AGENT_MODEL"] ?? "gpt-5.6" }),
      );
    }

    const anthropicKey = env["ANTHROPIC_API_KEY"];
    if (anthropicKey !== undefined && anthropicKey !== "") {
      return Effect.succeed(
        anthropicModel({
          apiKey: anthropicKey,
          ...(env["FOLD_AGENT_MODEL"] === undefined ? {} : { model: env["FOLD_AGENT_MODEL"] }),
        }),
      );
    }

    return Effect.fail(missingCredentials());
  });
