# Changelog

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
