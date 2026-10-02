import { storageDialectSuite } from "./storage-suite.ts";
import { sqlDialects } from "./support.ts";

storageDialectSuite(sqlDialects.mysql);
