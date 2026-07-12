#!/usr/bin/env python
"""
Script to identify CMSSW releases that do not support token-based authentication.

This script analyzes CMSSW releases to determine which (CMSSW version, architecture)
pairs support token-based authentication based on both CMSSW and XRootD version requirements.
The primary purpose is to identify releases that fail token readiness checks.

Requirements:
- CVMFS must be available and accessible at /cvmfs

What this script does:
1. Queries CMSSDT ReleasesXML to get all CMSSW versions and their production architectures
2. Filters CMSSW versions based on threshold (MIN_CMSSW_FOR_TOKENS = CMSSW_10_6_47)
3. For each (CMSSW version, architecture) pair:
   - Reads the xrootd.xml file from CVMFS
   - Extracts the XRootD version
   - Determines if tokens are enabled based on XRootD threshold (MIN_XROOTD_FOR_TOKENS = 5.7.2)
4. Creates triples: (CMSSW_version, XRootD_version, token_enabled)
5. Prints summary statistics of releases with/without token support
6. Optionally outputs a JSON list of CMSSW versions that pass CMSSW check but fail XRootD check

Version thresholds:
- CMSSW: Versions >= CMSSW_10_6_47 are considered
- XRootD: Versions >= 5.7.2 (base version, hash suffixes ignored) enable tokens
"""

import xml.etree.ElementTree as ET
import urllib.request
import json
import os
import sys
import argparse
from collections import defaultdict
import re

# Minimal CMSSW release that supports token-based authentication
MIN_CMSSW_FOR_TOKENS = "CMSSW_10_6_47"

# Minimal XRootD version that supports token-based authentication
# Note: Suffixes (build numbers) are disregarded - only base version (5.7.2) matters
MIN_XROOTD_FOR_TOKENS = "5.7.2"


def is_cmssw_token_ready(cmssw_candidate, cmssw_baseline=MIN_CMSSW_FOR_TOKENS):
    """
    Check if a CMSSW version is valid for token-based authentication.
    
    Based on the logic from WMCore.WMRuntime.Tools.Scram.isCMSSWSupported,
    this function determines if the candidate CMSSW version meets or exceeds
    the baseline version required for token support.
    
    The base version comparison (major.minor.patch) follows the same logic as
    isCMSSWSupported. For versions with the same base, versions with suffixes
    (patches) are considered newer than the base version.

    :param cmssw_candidate: CMSSW version string to test
    :param cmssw_baseline: Baseline CMSSW version string to compare against (default: MIN_CMSSW_FOR_TOKENS)
    :return: True if cmssw_candidate >= cmssw_baseline (valid for tokens), False otherwise
             Returns False if either version cannot be parsed
    """
    if not cmssw_baseline or not cmssw_candidate:
        return False

    # Exact match (including suffixes)
    if cmssw_candidate == cmssw_baseline:
        return True

    # Extract base version (major.minor.patch) - same approach as isCMSSWSupported
    try:
        parts_candidate = cmssw_candidate.split('_', 4)
        parts_baseline = cmssw_baseline.split('_', 4)
        
        if len(parts_candidate) < 4 or len(parts_baseline) < 4:
            return False
            
        candidate_version = [int(i) for i in parts_candidate[1:4]]
        baseline_version = [int(i) for i in parts_baseline[1:4]]
    except (ValueError, IndexError):
        return False

    # Compare base versions using the same logic as isCMSSWSupported
    for idx in range(3):
        if candidate_version[idx] > baseline_version[idx]:
            return True
        elif candidate_version[idx] < baseline_version[idx]:
            return False
        # If equal, continue to next component

    # Base versions are equal, now compare suffixes
    # Extract suffix if present
    suffix_candidate = parts_candidate[4] if len(parts_candidate) > 4 else None
    suffix_baseline = parts_baseline[4] if len(parts_baseline) > 4 else None

    # If both have no suffix, they're equal
    if suffix_candidate is None and suffix_baseline is None:
        return True

    # Versions with suffixes are considered newer than base versions
    if suffix_candidate is None and suffix_baseline is not None:
        return False  # candidate (base) < baseline (with suffix)
    if suffix_candidate is not None and suffix_baseline is None:
        return True  # candidate (with suffix) > baseline (base)

    # Both have suffixes - compare lexicographically
    if suffix_candidate < suffix_baseline:
        return False
    if suffix_candidate > suffix_baseline:
        return True

    return True


def parse_xrootd_version(xrootd_version):
    """
    Parse an XRootD version string into its components.
    
    Handles multiple formats:
    - "5.7.2" (base version)
    - "5.7.2.1" (with build number)
    - "5.7.2-b136d09c949edb14747a498380f172a4" (with hash suffix, hash is ignored)

    Examples:
        "5.7.2" -> (5, 7, 2, None)
        "5.7.2.1" -> (5, 7, 2, 1)
        "5.7.2-b136d09c949edb14747a498380f172a4" -> (5, 7, 2, None)
        "4.12.0" -> (4, 12, 0, None)

    :param xrootd_version: XRootD version string (e.g., "5.7.2" or "5.7.2-b136d09c949edb14747a498380f172a4")
    :return: Tuple of (major, minor, patch, build) or None if parsing fails
    """
    # Strip hash suffix if present (format: "5.7.2-hash" -> "5.7.2")
    # Hash suffixes are disregarded for version comparison
    if '-' in xrootd_version:
        xrootd_version = xrootd_version.split('-')[0]
    
    # Match pattern: MAJOR.MINOR.PATCH[.BUILD]
    parts = xrootd_version.split('.')
    if len(parts) >= 3:
        try:
            major = int(parts[0])
            minor = int(parts[1])
            patch = int(parts[2])
            build = int(parts[3]) if len(parts) > 3 else None
            return (major, minor, patch, build)
        except ValueError:
            return None
    return None


def is_xrootd_token_ready(xrootd_candidate, xrootd_baseline=MIN_XROOTD_FOR_TOKENS):
    """
    Check if an XRootD version is valid for token-based authentication.
    
    This function determines if the candidate XRootD version meets or exceeds
    the baseline version required for token support. When comparing against
    a baseline threshold (like MIN_XROOTD_FOR_TOKENS), build numbers and hash
    suffixes are disregarded - only the base version (major.minor.patch) is
    considered for threshold comparison.

    :param xrootd_candidate: XRootD version string to test (e.g., "5.7.3" or "5.7.2-b136d09c949edb14747a498380f172a4")
    :param xrootd_baseline: Baseline XRootD version string to compare against (default: MIN_XROOTD_FOR_TOKENS)
    :return: True if xrootd_candidate >= xrootd_baseline (valid for tokens), False otherwise
             Returns False if either version cannot be parsed
    """
    parsed_baseline = parse_xrootd_version(xrootd_baseline)
    parsed_candidate = parse_xrootd_version(xrootd_candidate)

    if parsed_baseline is None or parsed_candidate is None:
        return False

    major_baseline, minor_baseline, patch_baseline, build_baseline = parsed_baseline
    major_candidate, minor_candidate, patch_candidate, build_candidate = parsed_candidate

    # Compare major version
    if major_candidate < major_baseline:
        return False
    if major_candidate > major_baseline:
        return True

    # Compare minor version
    if minor_candidate < minor_baseline:
        return False
    if minor_candidate > minor_baseline:
        return True

    # Compare patch version
    if patch_candidate < patch_baseline:
        return False
    if patch_candidate > patch_baseline:
        return True

    # Base versions (major.minor.patch) are equal
    # For threshold comparison, build numbers are disregarded
    # If both have the same base version, they are considered equal (valid for tokens)
    return True


def fetch_releases_xml(url="https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML?anytype=1"):
    """Fetch the CMSSW releases XML from the given URL."""
    print(f"Fetching CMSSW releases XML from {url}...")
    with urllib.request.urlopen(url) as response:
        return response.read()


def parse_releases_xml(xml_content):
    """
    Parse the CMSSW releases XML and extract all CMSSW versions and their architectures.

    Returns a dictionary mapping CMSSW_version -> list of architectures.
    """
    print("Parsing XML...")
    root = ET.fromstring(xml_content)

    cmssw_arch_map = defaultdict(set)
    total_releases = 0

    # Iterate through all architecture elements
    for arch_elem in root.findall('.//architecture'):
        arch_name = arch_elem.get('name')

        # Find all projects in this architecture (any type, any state)
        for project in arch_elem.findall('.//project'):
            cmssw_version = project.get('label')
            if cmssw_version:
                cmssw_arch_map[cmssw_version].add(arch_name)
                total_releases += 1

    # Convert sets to sorted lists for consistency
    cmssw_arch_map = {version: sorted(architectures) 
                      for version, architectures in cmssw_arch_map.items()}

    print(f"Found {total_releases} total releases from CMSSDT")
    print(f"Found {len(cmssw_arch_map)} unique CMSSW versions")
    return cmssw_arch_map, total_releases


def filter_by_threshold(cmssw_arch_map, threshold_version=MIN_CMSSW_FOR_TOKENS):
    """
    Filter CMSSW versions based on threshold.

    Drops all versions below the threshold, keeps versions >= threshold (including threshold).

    :param cmssw_arch_map: Dictionary mapping CMSSW_version -> list of architectures
    :param threshold_version: CMSSW version threshold (default: MIN_CMSSW_FOR_TOKENS)
    :return: Tuple of (filtered_map, dropped_count)
    """
    print(f"Filtering CMSSW versions with threshold: {threshold_version}...")
    
    filtered_map = {}
    dropped_count = 0

    for cmssw_version, architectures in cmssw_arch_map.items():
        if is_cmssw_token_ready(cmssw_version, threshold_version):
            filtered_map[cmssw_version] = architectures
        else:
            # Version is below threshold or cannot be parsed
            dropped_count += 1

    print(f"Dropped {dropped_count} CMSSW versions below threshold")
    print(f"Kept {len(filtered_map)} CMSSW versions at or above threshold")
    return filtered_map, dropped_count


def check_cvmfs_available():
    """
    Check if CVMFS is available in the environment.
    
    :return: True if CVMFS is available, False otherwise
    """
    cvmfs_path = '/cvmfs'
    return os.path.exists(cvmfs_path) and os.path.isdir(cvmfs_path)


def is_patch_release(cmssw_version):
    """
    Check if a CMSSW version is a patch release.
    
    Patch releases can have various naming patterns:
    - CMSSW_12_4_14_patch1 (standard format)
    - CMSSW_12_4_21_HLT_patch1 (with suffix before patch)
    - CMSSW_15_1_0_HIN2patch3 (no underscore before patch)
    
    :param cmssw_version: CMSSW version string
    :return: True if this is a patch release, False otherwise
    """
    # Match patterns like: _patch1, _patch2, patch1, patch2, etc.
    # Look for "patch" followed by one or more digits
    pattern = r'patch\d+'
    return bool(re.search(pattern, cmssw_version, re.IGNORECASE))


def get_xrootd_xml_path(cmssw_version, architecture):
    """
    Build the path to xrootd.xml file for a given CMSSW version and architecture.
    
    Handles two path patterns:
    - Regular releases: /cvmfs/cms.cern.ch/{arch}/cms/cmssw/{CMSSW_VERSION}/...
    - Patch releases: /cvmfs/cms.cern.ch/{arch}/cms/cmssw-patch/{CMSSW_VERSION}/...
    
    Patch releases are detected by looking for "patch" followed by digits in the version string.
    Examples: CMSSW_10_6_47_patch2, CMSSW_12_4_21_HLT_patch1, CMSSW_15_1_0_HIN2patch3
    
    :param cmssw_version: CMSSW version string (e.g., "CMSSW_10_6_47" or "CMSSW_10_6_47_patch2")
    :param architecture: Architecture string (e.g., "slc7_amd64_gcc700")
    :return: Path to xrootd.xml file
    """
    # Check if this is a patch release
    if is_patch_release(cmssw_version):
        cmssw_dir = 'cmssw-patch'
    else:
        cmssw_dir = 'cmssw'
    
    return f"/cvmfs/cms.cern.ch/{architecture}/cms/{cmssw_dir}/{cmssw_version}/config/toolbox/{architecture}/tools/selected/xrootd.xml"


def parse_xrootd_xml(xml_path):
    """
    Parse xrootd.xml file and extract the XRootD version.
    
    :param xml_path: Path to xrootd.xml file
    :return: XRootD version string or None if file cannot be read/parsed
    """
    try:
        if not os.path.exists(xml_path):
            return None
        
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Check if root element is the tool element with name="xrootd"
        if root.tag == 'tool' and root.get('name') == 'xrootd':
            version = root.get('version')
            if version:
                return version
        
        # Otherwise, search for tool element with name="xrootd" in the tree
        tool = root.find('.//tool[@name="xrootd"]')
        if tool is not None:
            version = tool.get('version')
            if version:
                return version
        
        return None
    except (ET.ParseError, IOError, OSError):
        return None


def process_cmssw_arch_pairs(filtered_map):
    """
    Process all (CMSSW version, architecture) pairs to extract XRootD versions
    and determine token readiness.
    
    :param filtered_map: Dictionary mapping CMSSW_version -> list of architectures
    :return: List of tuples (CMSSW_version, XRootD_version, token_enabled)
    """
    print("\nProcessing CMSSW/architecture pairs to extract XRootD versions...")
    results = []
    processed_count = 0
    failed_count = 0
    
    for cmssw_version, architectures in sorted(filtered_map.items()):
        for arch in architectures:
            processed_count += 1
            xml_path = get_xrootd_xml_path(cmssw_version, arch)
            xrootd_version = parse_xrootd_xml(xml_path)
            
            if xrootd_version is None:
                # File doesn't exist or cannot be parsed
                failed_count += 1
                token_enabled = False
            else:
                token_enabled = is_xrootd_token_ready(xrootd_version)
            
            results.append((cmssw_version, xrootd_version, token_enabled))
            
            if processed_count % 100 == 0:
                print(f"  Processed {processed_count} pairs...")
    
    print(f"Processed {processed_count} CMSSW/architecture pairs")
    if failed_count > 0:
        print(f"  Failed to read/parse {failed_count} xrootd.xml files")
    
    return results


def print_debug_triples(results):
    """
    Print debugging information about token-enabled and token-disabled triples.
    
    :param results: List of tuples (CMSSW_version, XRootD_version, token_enabled)
    """
    token_enabled_triples = [(cmssw, xrootd, enabled) for cmssw, xrootd, enabled in results if enabled]
    token_disabled_triples = [(cmssw, xrootd, enabled) for cmssw, xrootd, enabled in results if not enabled]
    
    print(f"\n{'='*80}")
    print("DEBUG: Token-Enabled Triples")
    print(f"{'='*80}")
    if token_enabled_triples:
        for cmssw, xrootd, enabled in sorted(token_enabled_triples):
            xrootd_str = xrootd if xrootd else "N/A (file not found)"
            print(f"  {cmssw:30s} | XRootD: {xrootd_str:40s} | Tokens: {enabled}")
    else:
        print("  (none)")
    
    print(f"\n{'='*80}")
    print("DEBUG: Token-Disabled Triples")
    print(f"{'='*80}")
    if token_disabled_triples:
        for cmssw, xrootd, enabled in sorted(token_disabled_triples):
            xrootd_str = xrootd if xrootd else "N/A (file not found)"
            print(f"  {cmssw:30s} | XRootD: {xrootd_str:40s} | Tokens: {enabled}")
    else:
        print("  (none)")
    print(f"{'='*80}\n")


def get_cmssw_versions_failing_xrootd_check(results, filtered_map):
    """
    Extract CMSSW versions that pass the CMSSW version check but fail the XRootD check.
    
    These are CMSSW versions that:
    - Pass the initial CMSSW minimal version threshold (>= MIN_CMSSW_FOR_TOKENS)
    - But do NOT pass the XRootD version check (< MIN_XROOTD_FOR_TOKENS)
    
    :param results: List of tuples (CMSSW_version, XRootD_version, token_enabled)
    :param filtered_map: Dictionary of CMSSW versions that passed the CMSSW threshold
    :return: Sorted list of unique CMSSW version strings
    """
    # Get all CMSSW versions that passed the CMSSW threshold
    cmssw_passed_threshold = set(filtered_map.keys())
    
    # Get CMSSW versions that failed XRootD check (token_enabled = False)
    cmssw_failed_xrootd = set()
    for cmssw, xrootd, token_enabled in results:
        if not token_enabled:
            cmssw_failed_xrootd.add(cmssw)
    
    # Find intersection: versions that passed CMSSW check but failed XRootD check
    failing_versions = cmssw_passed_threshold & cmssw_failed_xrootd
    
    return sorted(failing_versions)


def dump_failing_versions_to_json(failing_versions, output_file):
    """
    Write CMSSW versions that fail XRootD check to a JSON file.
    
    :param failing_versions: List of CMSSW version strings
    :param output_file: Path to output JSON file
    """
    print(f"\nWriting {len(failing_versions)} CMSSW versions to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(failing_versions, f, indent=2, sort_keys=True)
    print(f"Successfully wrote CMSSW versions to {output_file}")


def main():
    """Main function."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Determine token-based authentication readiness for CMSSW releases'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Print debugging information: triples with and without token support'
    )
    parser.add_argument(
        '--output-json',
        type=str,
        default=None,
        help='Output JSON file path for CMSSW versions that pass CMSSW check but fail XRootD check'
    )
    args = parser.parse_args()
    
    # Step 1: Fetch and parse XML to get all CMSSW versions and architectures
    xml_content = fetch_releases_xml()
    cmssw_arch_map, total_releases = parse_releases_xml(xml_content)

    # Step 2: Filter by threshold (MIN_CMSSW_FOR_TOKENS)
    filtered_map, dropped_count = filter_by_threshold(cmssw_arch_map, MIN_CMSSW_FOR_TOKENS)

    # Print summary statistics
    print(f"\nSummary Statistics:")
    print(f"  Total releases retrieved from CMSSDT: {total_releases}")
    print(f"  Total CMSSW versions dropped: {dropped_count}")
    print(f"  Total CMSSW versions considered for next step: {len(filtered_map)}")

    # Step 3: Check if CVMFS is available before processing pairs
    print("\nChecking CVMFS availability...")
    if not check_cvmfs_available():
        print("ERROR: CVMFS is not available in this environment.")
        print("       The /cvmfs directory does not exist or is not accessible.")
        print("       This script requires CVMFS to access CMSSW release files.")
        sys.exit(1)
    print("CVMFS is available.")

    # Step 4: Process all (CMSSW, architecture) pairs to extract XRootD versions
    results = process_cmssw_arch_pairs(filtered_map)
    
    # Step 5: Print summary of token readiness
    token_enabled_count = sum(1 for _, _, enabled in results if enabled)
    token_disabled_count = len(results) - token_enabled_count
    
    print(f"\nToken Readiness Summary:")
    print(f"  Total (CMSSW, architecture) pairs processed: {len(results)}")
    print(f"  Releases with tokens enabled: {token_enabled_count}")
    print(f"  Releases with tokens disabled: {token_disabled_count}")
    
    # Print debug information if requested
    if args.debug:
        print_debug_triples(results)
    
    # Dump failing versions to JSON if requested
    if args.output_json:
        failing_versions = get_cmssw_versions_failing_xrootd_check(results, filtered_map)
        dump_failing_versions_to_json(failing_versions, args.output_json)


if __name__ == '__main__':
    main()
