# ChatGPT Prompts — YottaFlex AI Data Engineering Hackathon

Order of the work: understanding the problem, planning, building, testing, improving, and preparing the submission.

## 1. Understand the problem statement

### Prompt 1

Act as a senior Python data engineer. I am building a reusable data pipeline framework. First, analyze the project requirements. Do not generate code yet. Explain:
1. Recommended architecture
2. Folder structure
3. Responsibilities of each module
4. Data flow
5. Testing strategy
Keep the implementation beginner-friendly, modular, reusable and suitable for local execution.

### Prompt 2

You are an expert Python Data Engineer helping me build a professional hackathon project called YottaFlex AI Data Pipeline Framework. First, analyze these requirements and propose a clean, modular, professional Python project architecture. Do not generate all the implementation code yet. Explain the purpose of each folder and file. Wait for my approval before creating the project files.

### Prompt 3 — Understand the requirements

I have shared the YottaFlex hackathon problem statement. Can you go through it carefully and explain what exactly I need to build? Break the requirements into smaller tasks and tell me what the final solution should demonstrate. Please cover extraction, validation, transformation, loading, logging, metrics, two datasets, error handling, testing, performance, documentation, and GitHub submission. Also point out the important edge cases I should not miss.

## 2. Plan the project

### Prompt 4 — Plan the folder structure

Based on the requirements, can you suggest a clean folder structure for this project? I want separate modules for extraction, validation, transformation, loading, orchestration, database handling, logging, and metrics. Explain what each file should do. Please keep the design modular and easy for me to understand and maintain. Don't create all the files until I approve the structure.

### Prompt 5 — Define the datasets and database

Help me decide the source and target schemas for the customer and order datasets. Explain the columns, data types, primary keys, foreign keys, required fields, and business rules. I want the design to handle invalid data and duplicates properly. Please also suggest the SQLite tables needed to track pipeline runs, accepted records, rejected records, errors, and metrics.

## 3. Build the pipeline step by step

### Prompt 6 — Create the database module

Let's start with the database part. Help me create a reusable SQLite module with connection handling, foreign-key enforcement, schema initialization, transactions, and proper cleanup. Explain the code as we go so I can understand how each part works. Keep the database logic separate from the other pipeline modules.

### Prompt 7 — Build the extraction module

Now help me build the CSV extraction module for customers and orders. It should check whether the input file exists, validate the headers, handle empty files and malformed rows, and keep the source filename and row number for traceability. Please explain how the extracted data will be passed to the next stage.

### Prompt 8 — Build the validation modules

Help me implement the validation stage for both datasets. Check required fields, data types, email format, dates, timestamps, country codes, allowed status values, positive integer amounts, duplicate IDs, and customer references. I want valid records and rejected records to be separated, with a clear reason for every rejection and enough source information to trace it back to the original CSV row.

### Prompt 9 — Build the transformation module

Let's implement the transformation stage next. Please normalize the data consistently, convert timestamps to UTC, store monetary values as integer cents, keep phone numbers as text, and preserve source traceability. Make sure the transformations are deterministic and explain why each one is needed.

### Prompt 10 — Build the loading module

Help me implement the loading stage using SQLite. I need safe reruns and idempotency: if the same record with the same content is loaded again, it should be treated as unchanged; if the same ID comes with changed content, handle it according to the project rules rather than silently overwriting it. Also handle foreign-key failures, accepted and rejected audit records, metrics, and transaction rollback. Please explain how retries will work.

### Prompt 11 — Connect everything with orchestration

Now help me connect extraction, validation, transformation, and loading into one end-to-end pipeline for both customers and orders. I want a command-line interface where I can provide the input CSV paths and database path. Track the run status, row counts, rejected records, errors, and reconciliation results. Keep the individual modules reusable.

## 4. Generate data, test, and improve

### Prompt 12 — Generate reproducible sample data

Can you help me create a synthetic data generator for customers and orders? I want to choose how many records to generate and provide a random seed so the output is reproducible. Please make sure the generated orders refer to valid customers and follow the schemas. Keep generated data separate from the small sample files used for demonstrations.

### Prompt 13 — Write automated tests

Help me create unit and integration tests for this framework using pytest. Please cover normal successful runs as well as empty files, missing columns, malformed rows, null values, duplicates, invalid customer references, invalid dates and amounts, reruns, and failures during loading. Explain what each test is checking and how I can run the full test suite.

### Prompt 14 — Debug errors with me

I will share my code, screenshot, or error output. Please first understand what is happening and identify the actual cause. Explain the issue in simple terms, then suggest the smallest safe fix. Don't assume anything about files or code I haven't shared, and don't change unrelated parts. After the fix, give me one exact command to run so I can verify it before moving to the next step.

### Prompt 15 — Benchmark the pipeline

I want to measure how well the pipeline performs with larger datasets. Help me create a repeatable benchmark with configurable customer and order counts and multiple iterations. Measure data generation separately from extraction, validation, transformation, loading, and total pipeline time. Save the results in JSON and CSV, calculate throughput, and help me identify the main bottleneck. Don't invent hardware details or performance claims.

## 5. Prepare the documentation and submission

### Prompt 16 — Prepare project documentation

Can you review the completed project and help me prepare the README and supporting documentation? Explain the architecture, data flow, schemas, assumptions, validation rules, database tables, idempotency, error handling, metrics, testing, benchmark results, setup instructions, run commands, known limitations, and possible next steps. Please make sure the documentation matches the actual code and results.

### Prompt 17 — Final project review

Before I submit this hackathon project, can you help me check whether everything is ready? Review the source code, dependency files, sample data, generator, tests, execution evidence, output reports, reconciliation results, README, and GitHub structure. Give me a checklist of anything missing. Please don't say something is complete unless we have actual evidence for it.

## 6. How I used ChatGPT

I used ChatGPT as a learning, planning, and development assistant throughout the project. I asked it to help me understand the problem statement, plan the architecture, break the work into manageable steps, explain unfamiliar concepts, review code, troubleshoot errors, suggest test cases, interpret execution results, and prepare documentation and also i used chatgpt for helping me prompts for GitHub Copiolt also.
