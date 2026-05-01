import { createStateSchema, type ChangeEvent } from "@durable-streams/state";
import { z } from "zod";

export const hnStorySchema = z.object({
  id: z.number(),
  by: z.string().optional(),
  descendants: z.number().optional(),
  score: z.number().optional(),
  time: z.number(),
  title: z.string(),
  type: z.literal("story"),
  url: z.string().optional(),
  text: z.string().optional(),
});

export const hackerNewsState = createStateSchema({
  stories: {
    schema: hnStorySchema,
    type: "hn-story",
    primaryKey: "id",
  },
});

export type HnStory = z.infer<typeof hnStorySchema>;
export type HnStateEvent = ChangeEvent<HnStory>;
