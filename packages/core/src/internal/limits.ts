/**
 * The MySQL producer key holds a stream id and at most this many producer id
 * code points, so every storage dialect enforces it. Projections that build
 * their own producer ids keep them within it.
 */
export const MAX_PRODUCER_ID_LENGTH = 255;
