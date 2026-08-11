import 'dart:convert';
import 'dart:io';

import 'package:uuid/uuid.dart';

/// Generates an SBOM for the core extension.
///
/// Usage: `dart tool/generate_sbom.dart > bom.json`
void main() async {
  final [coreExtension, ...dependencies] = await findDependencies();

  const journeyApps = {
    'name': 'JourneyApps',
    'url': ['https://powersync.com', 'https://journeyapps.com/'],
  };

  final sbom = {
    'bomFormat': 'CycloneDX',
    'specVersion': '1.7',
    'serialNumber': 'urn:uuid:${const Uuid().v4()}',
    if (Platform.environment['GITHUB_ACTIONS'] == 'true')
      'version': int.parse(Platform.environment['GITHUB_RUN_ID']!),
    'metadata': {
      'component': coreExtension.describeAsBomComponent(),
      'lifecycles': [
        {'phase': 'build'}
      ],
      'timestamp': DateTime.now().toIso8601String(),
      'manufacturer': journeyApps,
      'supplier': journeyApps,
    },
    'components': [
      for (final crate in dependencies) crate.describeAsBomComponent(),
      // Also declare SQLite as an external component required at runtime (since
      // this is a SQLite extension).
      {
        'isExternal': true,
        'versionRange': 'vers:semver/>=3.44.0|<4.0.0',
        'type': 'library',
        'name': 'SQLite',
        'purl': 'pkg:generic/sqlite',
        'bom-ref': 'external-sqlite',
        'licenses': [
          {'expression': 'blessing'},
        ],
        'externalReferences': [
          {'url': 'https://sqlite.org/', 'type': 'website'},
        ],
      }
    ],
    'dependencies': [
      for (final crate in [coreExtension, ...dependencies])
        if (crate.dependencies.isNotEmpty)
          {
            'ref': crate.bomRef,
            'dependsOn': [
              for (final dep in crate.dependencies) dep.bomRef,
              if (crate == coreExtension) 'external-sqlite'
            ]
          }
    ],
  };

  print(JsonEncoder.withIndent(' ' * 2).convert(sbom));
}

final class RustCrate {
  final String name;
  final String version;
  final String licenseExpression;
  final String repository;

  final List<RustCrate> dependencies = [];

  String get bomRef => '$name-$version';

  RustCrate({
    required this.name,
    required this.version,
    required this.licenseExpression,
    required this.repository,
  });

  Map<String, Object?> describeAsBomComponent() {
    return {
      'version': version,
      'type': 'library',
      'bom-ref': bomRef,
      'name': name,
      'scope': 'required',
      'licenses': [
        {'expression': licenseExpression},
      ],
      'purl': 'pkg:cargo/${name}@${version}',
      'externalReferences': [
        {'url': repository, 'type': 'vcs'},
        if (name.contains('powersync'))
          {
            'url': 'https://powersync.com/',
            'type': 'website',
          }
      ],
    };
  }
}

// Matches a single line of `cargo tree --prefix depth` output, e.g.
// `1serde v1.0.228;MIT OR Apache-2.0;https://github.com/serde-rs/serde` or
// `0powersync_core v0.5.2 (/path/to/crates/core);Apache-2.0;https://...`.
// Cargo dedupes subtrees it has already expanded elsewhere by appending
// ` (*)` and not descending further, which the trailing group strips.
final _treeLine = RegExp(
  r'^(\d+)(\S+) v(\S+)(?: \([^)]*\))?;([^;]*);(.*?)(?: \(\*\))?$',
);

Future<List<RustCrate>> findDependencies() async {
  final result = await Process.run('cargo', [
    'tree',
    '-p',
    'powersync_core',
    '--edges',
    'normal,no-proc-macro',
    '--prefix',
    'depth',
    '--format',
    '{p};{l};{r}',
  ]);
  if (result.exitCode != 0) {
    throw 'cargo tree failed with exit code ${result.exitCode}:\n${result.stderr}';
  }

  final crates = <String, RustCrate>{};
  // The chain of crates from the root to the current line, indexed by depth.
  final stack = <RustCrate>[];

  for (final line in const LineSplitter().convert(result.stdout as String)) {
    if (line.trim().isEmpty) {
      continue;
    }

    final match = _treeLine.firstMatch(line);
    if (match == null) {
      throw 'Could not parse cargo tree line: $line';
    }

    final depth = int.parse(match[1]!);
    final name = match[2]!;
    final version = match[3]!;
    final license = match[4]!;
    final repository = match[5]!;

    // A crate is only fully expanded the first time it's encountered, so
    // later (deduped) occurrences reuse the same instance and its already
    // populated dependencies.
    final crate = crates.putIfAbsent(
      '$name@$version',
      () => RustCrate(
        name: name,
        version: version,
        licenseExpression: license,
        repository: repository,
      ),
    );

    if (depth < stack.length) {
      stack.removeRange(depth, stack.length);
    }
    if (depth > 0) {
      final parent = stack[depth - 1];
      if (!parent.dependencies.contains(crate)) {
        parent.dependencies.add(crate);
      }
    }
    stack.add(crate);
  }

  return crates.values.toList();
}
