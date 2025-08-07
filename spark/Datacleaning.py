#!/usr/bin/env python
# coding: utf-8

# ### data import

# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, expr, regexp_replace, split, size, slice,
    lower, trim, when, year, month, dayofweek, hour, length,
    udf, lit, concat_ws, substring_index, max as spark_max
)
from pyspark.sql.types import *

# ----------------------------------------------------------------------------------
# 1. Spark session + logging
# ----------------------------------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("CleanGitHubCommits")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
log = spark._jvm.org.apache.log4j.LogManager.getLogger("CleanGitHubCommits")

# In[3]:


from pyspark.sql.functions import from_json, col, regexp_extract

# ----------------------------------------------------------------------------------
# 2. Schema – only keep what we actually need
# ----------------------------------------------------------------------------------
file_schema = StructType([
    StructField("filename", StringType(), True),
    StructField("status", StringType(), True),
    StructField("additions", IntegerType(), True),
    StructField("deletions", IntegerType(), True)
])

commit_schema = StructType([
    StructField("sha", StringType(), True),
    StructField("commit", StructType([
        StructField("author", StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("date", StringType(), True)
        ]), True),
        StructField("committer", StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("date", StringType(), True)
        ]), True),
        StructField("message", StringType(), True)
    ]), True),
    StructField("html_url", StringType(), True),
    StructField("files", ArrayType(file_schema), True)
])

# ----------------------------------------------------------------------------------
# 3. Load raw Kafka messages
# ----------------------------------------------------------------------------------
df_raw = (
    spark.read
         .format("kafka")
         .option("kafka.bootstrap.servers", "kafka:29092")
         .option("subscribe", "github-commits")
         .option("startingOffsets", "earliest")
         .option("endingOffsets", "latest")
         .load()
)

count = df_raw.count()

# ----------------------------------------------------------------------------------
# 4. Enrich df_raw with repo name
# ----------------------------------------------------------------------------------
# Parse the Kafka JSON value
df_json = (
    df_raw.select(from_json(col("value").cast("string"), commit_schema).alias("c"))
           .select("c.*")
)
# Extract repo from html_url
df_json= df_json.withColumn(
    "repo",
    regexp_extract(col("html_url"), r"github\.com/([^/]+/[^/]+)", 1)
)

count = df_json.count()
print(f"Successfully loaded {count} records from topic 'github-commits'.")



# ### data cleaning

# In[4]:



# Kafka’s `value` is binary; cast to string and parse JSON


# ----------------------------------------------------------------------------------
# 4. Helper UDFs / expressions
# ----------------------------------------------------------------------------------


# Simple language inference from filename extension
ext2lang = {
    # Scripting / dynamic
    "py": "python",
    "pyw": "python",
    "js": "javascript",
    "ts": "typescript",
    "rb": "ruby",
    "php": "php",
    "pl": "perl",
    "pm": "perl",
    "t": "perl",
    "sh": "shell",
    "bash": "shell",
    "zsh": "shell",
    "ksh": "shell",
    "fish": "shell",
    "bat": "batch",
    "cmd": "batch",
    "ps1": "powershell",
    "psm1": "powershell",
    "awk": "awk",
    "tcl": "tcl",
    "rexx": "rexx",

    # Compiled languages
    "java": "java",
    "kt": "kotlin",
    "kts": "kotlin",
    "scala": "scala",
    "c": "c",
    "h": "c",
    "cpp": "cpp",
    "cc": "cpp",
    "cxx": "cpp",
    "c++": "cpp",
    "hpp": "cpp",
    "hh": "cpp",
    "hxx": "cpp",
    "cs": "csharp",
    "go": "go",
    "rs": "rust",
    "swift": "swift",
    "m": "objective-c",
    "mm": "objective-c++",
    "d": "d",
    "nim": "nim",
    "ada": "ada",
    "adb": "ada",
    "ads": "ada",
    "for": "fortran",
    "f90": "fortran",
    "f95": "fortran",
    "f03": "fortran",
    "asm": "assembly",
    "s": "assembly",
    "v": "verilog",
    "vh": "verilog",
    "vhd": "vhdl",
    "vhdl": "vhdl",
    "zig": "zig",

    # Web / Markup / Style
    "html": "html",
    "htm": "html",
    "xhtml": "html",
    "css": "css",
    "scss": "scss",
    "sass": "sass",
    "less": "less",
    "json": "json",
    "json5": "json",
    "xml": "xml",
    "yaml": "yaml",
    "yml": "yaml",
    "md": "markdown",
    "markdown": "markdown",
    "rst": "restructuredtext",
    "asciidoc": "asciidoc",
    "adoc": "asciidoc",

    # Data / Config / Infra
    "toml": "toml",
    "ini": "ini",
    "cfg": "config",
    "conf": "config",
    "env": "dotenv",
    "properties": "properties",
    "gradle": "gradle",
    "make": "makefile",
    "mk": "makefile",
    "dockerfile": "dockerfile",
    "dockerignore": "dockerignore",
    "compose": "yaml",
    "tf": "terraform",
    "tfvars": "terraform",
    "hcl": "hcl",
    "nomad": "hcl",
    "bazel": "bazel",
    "bzl": "bazel",

    # Query & Data languages
    "sql": "sql",
    "psql": "sql",
    "graphql": "graphql",
    "gql": "graphql",
    "csv": "csv",
    "tsv": "tsv",
    "parquet": "parquet",
    "orc": "orc",

    # Functional & others
    "r": "r",
    "jl": "julia",
    "ml": "ocaml",
    "mli": "ocaml",
    "hs": "haskell",
    "lhs": "haskell",
    "purs": "purescript",
    "elm": "elm",
    "idris": "idris",
    "clj": "clojure",
    "cljs": "clojure",
    "edn": "edn",
    "lisp": "lisp",
    "el": "emacs-lisp",
    "scm": "scheme",
    "rkt": "racket",

    # VMs / Scripting extensions
    "lua": "lua",
    "dart": "dart",
    "coffee": "coffeescript",
    "vue": "vue",
    "jsx": "jsx",
    "tsx": "tsx",

    # BEAM / Erlang VM
    "erl": "erlang",
    "hrl": "erlang",
    "ex": "elixir",
    "exs": "elixir",

    # Miscellaneous
    "groovy": "groovy",
    "ivy": "xml",
    "proto": "protobuf",
    "thrift": "thrift",
    "avdl": "avro",
    "avsc": "avro",
    "ipynb": "jupyter",
    "tex": "latex",
    "sty": "latex",
    "cls": "latex",
    "Rmd": "rmarkdown",

    # Dotfiles / Tools
    "gitignore": "git",
    "gitattributes": "git",
    "editorconfig": "editorconfig",
    "npmrc": "npm",
    "yarnrc": "yarn",

    # Fallbacks
    "txt": "text",
    "log": "log",
    "in": "Makefile",        # often used in autoconf/automake systems
    "erb": "Embedded Ruby",           # Embedded Ruby templates
    "feature": "Gherkin",    # Used in BDD frameworks like Cucumber
    "po": "gettext"          # Translation files, structured like config/data
}

@udf("string")
def infer_lang(filename):
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ""
    return ext2lang.get(ext, "Other")


# Change‑type heuristic
change_expr = (
    when(lower(col("commit_message_clean")).rlike(r"\bfix(e[ds])?\b|bug"), "fix")
    .when(lower(col("commit_message_clean")).rlike(r"\brefactor|clean"), "refactor")
    .when(lower(col("commit_message_clean")).rlike(r"\b(add|feature|implement)"), "feature")
    .otherwise("other")
)




# In[5]:



@udf("string")
def infer_lang(filename):
    if not filename:
        return None
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ""
    # Basic filename-based heuristic for known extensionless files
    if filename.lower() in {"makefile", "dockerfile", "license"}:
        return filename.lower()
    return ext2lang.get(ext, "Other")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BooleanType

def is_bot(name_or_email):
    if name_or_email is None:
        return False
    val = name_or_email.lower()
    keywords = ["noreply", "bot", "ci", "automation", "auto", "deploy"]
    return any(k in val for k in keywords)

is_bot_udf = udf(is_bot, BooleanType())


# ----------------------------------------------------------------------------------
# 5. Flatten to file‑level rows
# ----------------------------------------------------------------------------------
df_exp = (
    df_json
      .withColumn("author_name_raw",  col("commit.author.name"))
      .withColumn("author_email_raw", col("commit.author.email"))
      .withColumn("committer_name",   col("commit.committer.name"))
      .withColumn("committer_email",  col("commit.committer.email"))
      .withColumn("commit_timestamp", col("commit.author.date").cast("timestamp"))
      .withColumn("commit_message_raw", col("commit.message"))
      .withColumn("files",            col("files"))
      .withColumn("author_name",      lower(trim(col("author_name_raw"))))
      .withColumn("author_email",     lower(col("author_email_raw")))
      .drop("author_name_raw", "author_email_raw")
      .withColumn("commit_message_clean",
                  lower(regexp_replace(col("commit_message_raw"), r"http\S+|\p{So}", "")))
      .withColumn("year",        year("commit_timestamp"))
      .withColumn("month",       month("commit_timestamp"))
      .withColumn("day_of_week", dayofweek("commit_timestamp") - 2)  # Spark: 1=Sun
      .withColumn("hour_of_day", hour("commit_timestamp"))
      .withColumn("file",        expr("explode(files)"))
      .selectExpr(
          "repo",
          "sha as commit_sha",
          "author_name",
          "author_email",
          "committer_name",
          "committer_email",
          "file.filename as file_path",
          "file.status as change_status",
          "file.additions as lines_added",
          "file.deletions as lines_deleted",
          "commit_message_clean",
          "commit_timestamp",
          "year","month","day_of_week","hour_of_day"
      )
      .filter(col("file_path").isNotNull())  # extra guard
      .filter(
        (~is_bot_udf(col("author_name"))) &
        (~is_bot_udf(col("author_email"))) &
        (~is_bot_udf(col("committer_name"))) &
        (~is_bot_udf(col("committer_email")))
    )
)
df_exp = df_exp.filter(col("file_path").isNotNull() & (col("file_path") != ""))

# ----------------------------------------------------------------------------------
# 6. File‑level enrichments
# ----------------------------------------------------------------------------------
df_clean = (
    df_exp
.withColumn("file_name", expr("element_at(split(file_path, '/'), -1)"))
      .withColumn("path_depth", size(split(col("file_path"), "/")) - 1)
      .withColumn("module_candidate",
                  concat_ws("/",
                            slice(split(col("file_path"), "/"), 1, 2)))  # first 1‑2 folders
      .withColumn("language", infer_lang(col("file_name")))
      .withColumn("commit_size", col("lines_added") + col("lines_deleted"))
      .withColumn("change_type", change_expr)
      .dropDuplicates(["commit_sha", "file_path"])  # defensive
)

count = df_clean.count()
print(f"Successfully cleaned data, count after cleaning: {count}.")

# In[6]:


from pyspark.sql.functions import col, udf, lower, trim, split, expr
from pyspark.sql.types import StringType

# Broadcast ext2lang keys to Spark
known_exts = set(ext2lang.keys())

# UDF to extract extension
@udf("string")
def get_extension(filename):
    if not filename:
        return None
    if filename.lower() in {"makefile", "dockerfile", "license"}:
        return filename.lower()
    return filename.rsplit(".", 1)[-1].lower() if '.' in filename else ""

# Add extension and filter unknowns
df_ext_check = df_clean.withColumn("ext", get_extension(col("file_name")))

# Filter where language is Other and extension is not in ext2lang
df_unknown_exts = (
    df_ext_check
    .filter((col("language") == "Other") & col("ext").isNotNull() & (col("ext") != ""))
    .select("ext")
    .distinct()
)

# Collect and print
unknown_extensions = [row["ext"] for row in df_unknown_exts.collect()]
print("Unknown extensions not in ext2lang dict:", unknown_extensions)


# ### module df preparation

# #### module data cleaning

# In[7]:


from pyspark.sql.functions import col, split, slice, concat_ws, unix_timestamp, lit, udf
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window
from math import log2
from pyspark.sql import SparkSession
import math
from pyspark.sql import functions as F

# === Config ===
MAX_DEPTH = 5
MIN_FILES_FOR_MODULE = 5
MAX_LANGUAGES_FOR_MODULE = 3
ENTROPY_THRESHOLD = 0.5

# --- Extract path components ---
df = df_clean.withColumn("path_parts", split(col("file_path"), "/"))
df = df.withColumn("depth", F.size(col("path_parts")) - 1)

# Create module candidates for depths 1..MAX_DEPTH
for d in range(1, MAX_DEPTH + 1):
    df = df.withColumn(f"module_depth_{d}", concat_ws("/", slice(col("path_parts"), 1, lit(d))))

# Flatten into (repo, file_path, module_candidate at depth d)
df_long = df.select(
    "repo", "file_path", "language", "commit_sha", "author_email",
    "commit_timestamp", "lines_added", "lines_deleted","author_name",
    F.explode(F.array([
        F.struct(lit(d).alias("depth_level"), col(f"module_depth_{d}").alias("module_candidate"))
        for d in range(1, MAX_DEPTH + 1)
    ])).alias("mod_struct")
).select(
    "repo", "file_path", "language", "commit_sha", "author_email", "commit_timestamp",
    "lines_added", "lines_deleted","author_name",
    col("mod_struct.depth_level").alias("depth_level"),
    col("mod_struct.module_candidate")
)

# --- Heuristic scoring to pick best module depth ---
df_scored = df_long.groupBy("repo", "module_candidate", "depth_level").agg(
    F.count("*").alias("file_count"),
    F.countDistinct("language").alias("language_count")
).withColumn(
    "score",
    col("file_count") * 0.6 + (MAX_LANGUAGES_FOR_MODULE - col("language_count")) * 0.4
)

# Pick best scoring module per (repo, module_candidate)
window_best = Window.partitionBy("repo", "module_candidate").orderBy(F.desc("score"))
df_best_modules = df_scored.withColumn("rank", F.row_number().over(window_best)).filter("rank = 1").drop("rank")

# Join best module assignment (keep depth_level here)
df_long_renamed = df_long.withColumnRenamed("depth_level", "long_depth_level")

df_filtered = df_long_renamed.join(
    df_best_modules.select("repo", "module_candidate", "depth_level").distinct(),
    on=["repo", "module_candidate"],
    how="inner"
)

# === Entropy Computation ===
df_commit_module = df_filtered.select("commit_sha", "module_candidate", "repo").distinct()

w_commit = Window.partitionBy("commit_sha")
df_entropy_piece = df_commit_module.withColumn("modules_in_commit", F.count("*").over(w_commit)).withColumn(
    "entropy_piece",
    F.when(col("modules_in_commit") == 1, lit(0.0)).otherwise(
        - (1 / col("modules_in_commit")) * F.log2(1 / col("modules_in_commit"))
    )
)

w_module = Window.partitionBy("repo", "module_candidate")
df_entropy_stats = df_entropy_piece.withColumn("entropy_sum", F.sum("entropy_piece").over(w_module)).withColumn(
    "commit_cnt", F.count("*").over(w_module)
).select("repo", "module_candidate", "entropy_sum", "commit_cnt").distinct().withColumn(
    "avg_entropy", col("entropy_sum") / col("commit_cnt")
)

df_entropy_filtered = df_entropy_stats.filter(col("avg_entropy") <= ENTROPY_THRESHOLD).select("repo", "module_candidate")
df_entropy_filtered = df_entropy_filtered.filter(~col("module_candidate").rlike(r"\\.[^/]+$"))

# === Pruning: Remove parent modules that contain submodules ===
df_parents = df_entropy_filtered.alias("parent")
df_children = df_entropy_filtered.alias("child")

def is_child(parent_path, child_path):
    return child_path.startswith(parent_path + "/")

is_child_udf = udf(is_child, BooleanType())

df_conflicts = df_parents.crossJoin(df_children).filter(
    (col("parent.module_candidate") != col("child.module_candidate")) &
    is_child_udf(col("parent.module_candidate"), col("child.module_candidate")) &
    (col("parent.repo") == col("child.repo"))
).select(
    col("parent.repo").alias("repo"), col("parent.module_candidate").alias("parent_module")
).distinct()

# Remove conflicted parent modules
df_entropy_filtered_alias = df_entropy_filtered.alias("df1")
df_conflicts_alias = df_conflicts.alias("df2")

df_entropy_filtered_pruned = df_entropy_filtered_alias.join(
    df_conflicts_alias,
    on=[
        col("df1.repo") == col("df2.repo"),
        col("df1.module_candidate") == col("df2.parent_module")
    ],
    how="left_anti"
).select(col("df1.repo"), col("df1.module_candidate"))

# === Preserve All Metadata ===
df_all_modules_assigned = df_filtered.select(
    "repo", "file_path", "module_candidate", "depth_level", "language", "commit_sha", "author_email",
    "commit_timestamp", "lines_added", "lines_deleted","author_name"
).distinct()

# Mark good modules (entropy + pruning passed)
df_good_modules = df_entropy_filtered_pruned.withColumn("is_good_module", lit(True))

# Flag assignments with good modules
df_all_with_flags = df_all_modules_assigned.join(
    df_good_modules,
    on=["repo", "module_candidate"],
    how="left"
).fillna({"is_good_module": False})

# Select best module per file based on heuristic
window_file = Window.partitionBy("repo", "file_path").orderBy(
    col("is_good_module").desc(),
    col("depth_level").asc()
)

df_clean_filtered = df_all_with_flags.withColumn("rank", F.row_number().over(window_file)).filter(
    col("rank") == 1
).drop("rank")

# === Sanity Checks ===
print("Modules before pruning:", df_entropy_filtered.count())
print("Modules after pruning:", df_entropy_filtered_pruned.count())
df_violations = df_entropy_filtered_pruned.alias("pruned").join(
    df_conflicts.alias("conflict"),
    (col("pruned.repo") == col("conflict.repo")) &
    (col("pruned.module_candidate") == col("conflict.parent_module")),
    how="inner"
)
print("Modules that violate pruning (should be 0):", df_violations.count())

print("Final modules assigned to files sample:")
df_clean_filtered.select("repo", "file_path", "module_candidate", "is_good_module").show(20, truncate=False)

# #### module data enrichement

# In[8]:


from pyspark.sql import functions as F
from pyspark.sql.functions import (
    collect_set,
    col,
    row_number,
    unix_timestamp
)
from pyspark.sql.window import Window

# === Per-file language list (for later aggregation) ===
df_lang_per_file = df_clean_filtered.select("repo", "module_candidate", "language")

# === Count all languages used per module ===
df_lang_all = df_lang_per_file.groupBy("repo", "module_candidate").agg(
    collect_set("language").alias("languages")
)

# === Dominant language per module ===
df_lang_counts = (
    df_lang_per_file.groupBy("repo", "module_candidate", "language")
    .agg(F.count("*").alias("lang_count"))
)

w_lang = Window.partitionBy("repo", "module_candidate").orderBy(col("lang_count").desc())
df_dominant_lang = (
    df_lang_counts.withColumn("rank", row_number().over(w_lang))
    .filter(col("rank") == 1)
    .select("repo", "module_candidate", col("language").alias("dominant_language"))
)

# === Module-level base stats ===
df_module_base = (
    df_clean_filtered.groupBy("repo", "module_candidate")
    .agg(
        F.countDistinct("author_email").alias("num_contributors"),
        F.countDistinct("commit_sha").alias("num_commits"),
        F.sum(F.col("lines_added") + F.col("lines_deleted")).alias("total_churn"),
        F.min("commit_timestamp").alias("first_commit_time"),
        F.max("commit_timestamp").alias("last_commit_time")
    )
)

# === Final module assembly ===
df_modules = (
    df_module_base
    .join(df_dominant_lang, on=["repo", "module_candidate"], how="left")
    .join(df_lang_all, on=["repo", "module_candidate"], how="left")
    .withColumn("age_days", (
        unix_timestamp("last_commit_time") - unix_timestamp("first_commit_time")
    ) / 86400)
    .withColumn("single_author_module", (col("num_contributors") == 1))
    .withColumn("popularity_score", 0.5 * col("num_contributors") + 0.5 * col("num_commits"))
    .withColumn("stability_score", 1 / (1 + col("total_churn")))
    .withColumn("maturity_score", col("age_days") / (col("age_days") + 30))
)

# === Optional: sanity check ===
other_count = df_dominant_lang.filter(col("dominant_language") == "Other").count()
print(f"Modules with 'Other' dominant language: {other_count / df_dominant_lang.count() * 100:.2f}%")

df_modules.select("repo", "module_candidate", "languages").show(truncate=False, n=20)


# In[9]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

N_DAYS = 90  # window for recent contributor flag

# 1. Aggregate contributions per author per module
df_contrib = (
    df_clean_filtered
      .groupBy("module_candidate", "author_name")
      .agg(
          F.count("commit_sha").alias("contributions"),
          F.max("commit_timestamp").alias("last_commit")
      )
      .withColumnRenamed("module_candidate", "module_path")
)

# 2. Join ALL languages info to each module_path
df_contrib_lang = df_contrib.join(
    df_lang_all,
    df_contrib.module_path == df_lang_all.module_candidate,
    how="left"
).drop("module_candidate")

# 3. Explode the list of languages
df_contrib_exploded = df_contrib_lang.withColumn("language", F.explode("languages"))

# 4. Calculate contributor ranking within each module based on contributions
w_module = Window.partitionBy("module_path").orderBy(F.desc("contributions"))

df_contrib_ranked = (
    df_contrib_exploded
      .withColumn("rank", F.row_number().over(w_module))
      .withColumn("primary_author", (F.col("rank") == 1))
      .withColumn(
          "recent_contributor",
          F.datediff(F.current_timestamp(), F.col("last_commit")) <= N_DAYS
      )
)

# 5. Aggregate per author and language for skill summary across all modules
df_author_lang_skill = (
    df_contrib_ranked
      .groupBy("author_name", "language")
      .agg(
          F.sum("contributions").alias("total_contributions"),
          F.max("last_commit").alias("last_commit")
      )
      .withColumn(
          "recent_contributor",
          F.datediff(F.current_timestamp(), F.col("last_commit")) <= N_DAYS
      )
)
print("Sample of author-language skill summary:")
df_author_lang_skill.select(
    "author_name", "language", "total_contributions", "recent_contributor"
).orderBy(F.desc("total_contributions")).show(20, truncate=False)

# Optional: count how many unique authors and languages we have
num_authors = df_author_lang_skill.select("author_name").distinct().count()
num_languages = df_author_lang_skill.select("language").distinct().count()

print(f"Total unique authors: {num_authors}")
print(f"Total unique languages: {num_languages}")


# # kafka producer

# In[10]:


from pyspark.sql.functions import to_json, struct, col

def write_df_to_kafka(df, topic_name, kafka_bootstrap="kafka:29092", key_col=None):
    if key_col:
        df_kafka = df.select(
            col(key_col).cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        )
    else:
        df_kafka = df.select(
            to_json(struct("*")).alias("value")
        )

    df_kafka.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", topic_name) \
        .save()
# 1. df_modules → no key
write_df_to_kafka(df_modules, topic_name="modules_summary",key_col="module_candidate")

# 2. df_contrib_ranked → key is "author_name"
write_df_to_kafka(df_contrib_ranked, topic_name="contributor_module_rank", key_col="author_name")

# 3. df_author_lang_skill → key is "author_name"
write_df_to_kafka(df_author_lang_skill, topic_name="contributor_skill_summary", key_col="author_name")


# In[11]:


print("df_modules columns:", df_modules.columns)
print("df_contrib_ranked columns:", df_contrib_ranked.columns)
print("df_author_lang_skill columns:", df_author_lang_skill.columns)

