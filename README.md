# Analytics Utilities

This project contains some of my most common utilities for doing an analysis on an A/b test

Most of the scripts here require that your data be formatted with the following columns: 
| id | assigned_at | conversion_at | variant |
| -- | -- | -- | -- | 
|uuid of assigned participant | datetime of assignment | datetime of conversion | which exposure the participant is in
