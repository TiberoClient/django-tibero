# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.2]

### Added
- N/A

### Changed
- Extended Python timedelta support: previously limited to inserting values and selecting columns,
  now supports arithmetic operations between timedelta values.

### Fixed
- Resolved an issue where timedelta objects with negative values did not behave correctly.
