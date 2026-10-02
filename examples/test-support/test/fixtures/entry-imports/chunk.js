import { base } from "chunk-bare";

export const value = base;
export const later = () => import("./lazy.js");
