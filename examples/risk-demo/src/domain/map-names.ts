/**
 * Versioned name pools for `hex-generator-v1`.
 *
 * These lists are part of the generator's contract: reordering, editing, or
 * extending them changes the output of every seed. Any change therefore needs a
 * new generator version. Names are drawn without replacement from the `names`
 * substream, so a pool must comfortably exceed the largest profile's counts
 * (20 territories, 4 continents).
 */

export const TERRITORY_NAME_POOL: readonly string[] = [
  "Ashfen",
  "Barrowmoor",
  "Blackmere",
  "Bramblewick",
  "Caldera",
  "Cinderhold",
  "Coldharbour",
  "Dunmarch",
  "Eastwatch",
  "Emberfall",
  "Fallowreach",
  "Farrowdale",
  "Fenwick",
  "Frostgate",
  "Gallowmoor",
  "Glimmerford",
  "Graymarch",
  "Greenhollow",
  "Hallowfen",
  "Harrowgate",
  "Hearthvale",
  "Highcairn",
  "Ironmoor",
  "Kestrelwatch",
  "Larkhollow",
  "Lowmeadow",
  "Marrowfield",
  "Millbrook",
  "Mistvale",
  "Netherby",
  "Northreach",
  "Oakenshade",
  "Palewater",
  "Quarrystone",
  "Ravenmoor",
  "Redcliff",
  "Rushmere",
  "Saltmarch",
  "Shalebrook",
  "Silverfen",
  "Stonewold",
  "Sunderhollow",
  "Thornwick",
  "Tidewatch",
  "Umbermoor",
  "Vellowmarsh",
  "Westhollow",
  "Windbarrow",
  "Wrackenfell",
  "Yarrowdeep",
];

export const CONTINENT_NAME_POOL: readonly string[] = [
  "Aldermark",
  "Brightsea Reach",
  "Cragmere",
  "Duskholm",
  "Eldergarth",
  "Fjordlend",
  "Goldenmarch",
  "Hollowreach",
  "Ironspan",
  "Kingsmoor",
  "Lastshore",
  "Verdanmoor",
];

/** Continent palette hues, assigned by continent index. Presentation metadata. */
export const CONTINENT_PALETTES: readonly { hue: number; pattern: string }[] = [
  { hue: 12, pattern: "diagonal" },
  { hue: 96, pattern: "dots" },
  { hue: 204, pattern: "cross" },
  { hue: 282, pattern: "wave" },
  { hue: 42, pattern: "grid" },
  { hue: 168, pattern: "chevron" },
];
