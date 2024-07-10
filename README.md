# sql-insight

**sql-insight** is a project designed to tackle recurring challenges encountered when working with SQL queries and extracting metadata.

## Milestones

### Core Functionality

- **Observer Strategy**: Implement a strategy for effectively traversing specific expressions within SQL queries. ✅
- **Path Search**: Develop a method to locate patterns within SQL expressions using a path-based approach. ✅
- **Recursive Expression Extraction**: Implement functionality to recursively extract expressions within SQL queries.

### Listeners

- **Partitioning Candidates**: Implement listeners to identify potential partitioning strategies within SQL queries.
- **Lineage Candidates**: Develop listeners to track data lineage within SQL queries.

### Autocomplete Utilities

- **Contextual Autocompletion**: Implement utilities to provide intelligent autocomplete suggestions based on context within SQL queries.

### Port into Typescript

- **Monaco SQL Language Support**: Integrate support for SQL syntax highlighting and language features within the Monaco editor.
- **Lineage Visualization?**: Implement functionality for visualizing data lineage to enhance understanding of SQL query dependencies.
