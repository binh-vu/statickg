# Changelog

## [1.7.0] - 2024-07-31

### Added

- Add helper functions to RelPath

## [1.6.4] - 2024-07-19

### Fixed

- Fix not handling CFG_DIR basepath

## [1.6.2] - 2024-07-12

### Fixed

- Fix incremental load

## [1.6.0] - 2024-07-12

### Added

- Parallel execution for D-REPR service by default (can be disabled)

### Changed

- Update D-REPR library for better performance

## [1.5.0] - 2024-07-01

### Added

- Function to find old versions of a database

## [1.4.1] - 2024-06-21

### Fixed

- Fix D-REPR service incorrectly using ETLOutput

## [1.4.0] - 2024-06-18

### Added

- Added utility functions to check/wait for a port to be available.

## [1.3.5] - 2024-06-18

### Fixed

- Fuseki service should create directory before calling load command

## [1.3.3] - 2024-06-18

### Fixed

- Fix bug in `find_available_port`

## [1.3.2] - 2024-06-18

### Fixed

- If fuseki fails to start, it automatically uses the stop command to clean up previous artifacts and try to start again.

### Changed

- Better error message telling what files was failed for D-REPR service

## [1.3.0] - 2024-06-09

### Fixed

- Fix Fuseki loading issues
- Fix pipeline storing services' output

### Changed

- Remove tracker & store outputs of previous tasks.

## [1.2.2] - 2024-06-09

### Added

- Improve change detection in Git repository.

## [1.2.1] - 2024-06-09

### Added

- Add missing dependency

## [1.2.0] - 2024-06-09

### Added

- Add version

### Fixed

- Handle detached HEAD in Git repository.
- Fix absolute path in ETL configuration

## [1.1.0] - 2024-05-29

### Added

- Add data loader for Apache Jena (Fuseki).

### Changed

- Use RelPath to avoid hardcoding paths in ETL pipeline.

## [1.0.1] - 2024-05-28

### Changed

- Upgrade D-REPR version
