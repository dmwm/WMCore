#!/usr/bin/env python
"""
Create a static mapping of CMSSW versions to their XrootD compatibility group.

This script:
1. Parses the CMSSW releases XML to get production architectures
2. Filters xrootd.txt to only production architectures
3. Creates a mapping of CMSSW version -> Group (True for Group 1, False for Group 2)
4. Saves the mapping to a static JSON file
"""

import xml.etree.ElementTree as ET
import urllib.request
import json
import os
from collections import defaultdict
import re


def get_cmssw_major_version(cmssw_version):
    """
    Extract the major version number from a CMSSW version string.

    Examples:
        CMSSW_8_1_2 -> 8
        CMSSW_9_0_0 -> 9
        CMSSW_10_6_47_patch1 -> 10
        CMSSW_12_4_24_patch1 -> 12

    :param cmssw_version: CMSSW version string
    :return: Major version number as integer, or None if parsing fails
    """
    # Match pattern: CMSSW_MAJOR_MINOR_PATCH...
    match = re.match(r'CMSSW_(\d+)_', cmssw_version)
    if match:
        return int(match.group(1))
    return None


def fetch_releases_xml(url="https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML"):
    """Fetch the CMSSW releases XML from the given URL."""
    print(f"Fetching CMSSW releases XML from {url}...")
    with urllib.request.urlopen(url) as response:
        return response.read()


def parse_releases_xml(xml_content):
    """
    Parse the CMSSW releases XML and extract production architecture mappings.

    Returns a set of (CMSSW_version, architecture) tuples.
    """
    print("Parsing XML...")
    root = ET.fromstring(xml_content)

    production_pairs = set()

    # Iterate through all architecture elements
    for arch_elem in root.findall('.//architecture'):
        arch_name = arch_elem.get('name')

        # Find all production projects in this architecture
        for project in arch_elem.findall('.//project[@type="Production"][@state="Announced"]'):
            cmssw_version = project.get('label')
            if cmssw_version:
                production_pairs.add((cmssw_version, arch_name))

    print(f"Found {len(production_pairs)} production CMSSW/architecture pairs")
    return production_pairs


def parse_xrootd_file(xrootd_file):
    """
    Parse the xrootd.txt file and return a dictionary mapping
    (CMSSW_version, architecture) -> group (True for Group 1, False for Group 2).
    """
    print(f"Reading {xrootd_file}...")
    xrootd_data = {}

    with open(xrootd_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            # Parse: CMSSW_version,architecture,xrootd_version,group
            parts = line.split(',')
            if len(parts) >= 4:
                cmssw_version = parts[0].strip()
                architecture = parts[1].strip()
                group_str = parts[3].strip()

                # Determine group: True for Group 1, False for Group 2
                is_group1 = group_str.startswith('Group 1')
                xrootd_data[(cmssw_version, architecture)] = is_group1

    print(f"Parsed {len(xrootd_data)} entries from xrootd.txt")
    return xrootd_data


def create_cmssw_group_map(production_pairs, xrootd_data):
    """
    Create a mapping of CMSSW version -> Group (True for Group 1, False for Group 2).

    Only includes CMSSW versions that have production architectures.
    If a CMSSW version has multiple architectures with different groups,
    we use the most common group, or Group 1 if there's a tie.

    Note: CMSSW versions below 9.x are always marked as Group 2 (False),
    regardless of the xrootd.txt data.
    """
    print("Creating CMSSW group mapping...")

    # First, collect all groups for each CMSSW version
    cmssw_groups = defaultdict(list)

    for (cmssw, arch) in production_pairs:
        if (cmssw, arch) in xrootd_data:
            is_group1 = xrootd_data[(cmssw, arch)]
            cmssw_groups[cmssw].append(is_group1)

    # For each CMSSW version, determine the group
    # Use the most common group, or Group 1 (True) if there's a tie
    cmssw_group_map = {}
    forced_group2_count = 0

    for cmssw, groups in cmssw_groups.items():
        # Check if this is a version below 9.x - force to Group 2
        major_version = get_cmssw_major_version(cmssw)
        if major_version is not None and major_version < 9:
            cmssw_group_map[cmssw] = False
            forced_group2_count += 1
            continue

        # Count Group 1 vs Group 2
        group1_count = sum(groups)
        group2_count = len(groups) - group1_count

        # Use Group 1 if it's more common or if there's a tie
        cmssw_group_map[cmssw] = group1_count >= group2_count

    print(f"Created mapping for {len(cmssw_group_map)} CMSSW versions")
    if forced_group2_count > 0:
        print(f"  Forced {forced_group2_count} CMSSW versions < 9.x to Group 2")
    return cmssw_group_map


def save_group_map(group_map, output_file):
    """Save the CMSSW group mapping to a JSON file."""
    print(f"Saving mapping to {output_file}...")

    with open(output_file, 'w') as f:
        json.dump(group_map, f, indent=2, sort_keys=True)

    # Print statistics
    group1_count = sum(1 for is_group1 in group_map.values() if is_group1)
    group2_count = len(group_map) - group1_count

    print(f"\nStatistics:")
    print(f"  Total CMSSW versions: {len(group_map)}")
    print(f"  Group 1 (>= 5.7.2): {group1_count}")
    print(f"  Group 2 (< 5.7.2): {group2_count}")
    print(f"  Saved to: {output_file}")


def main():
    """Main function."""
    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    xrootd_file = os.path.join(script_dir, 'xrootd.txt')
    output_file = os.path.join(script_dir, 'cmssw_token_readiness_map.json')

    # Step 1: Fetch and parse XML to get production architectures
    xml_content = fetch_releases_xml()
    production_pairs = parse_releases_xml(xml_content)

    # Step 2: Parse xrootd.txt
    xrootd_data = parse_xrootd_file(xrootd_file)

    # Step 3: Create CMSSW group mapping (only for production architectures)
    cmssw_group_map = create_cmssw_group_map(production_pairs, xrootd_data)

    # Step 4: Save the mapping
    save_group_map(cmssw_group_map, output_file)


if __name__ == '__main__':
    main()
