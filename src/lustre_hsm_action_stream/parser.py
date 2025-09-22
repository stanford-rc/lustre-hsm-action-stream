# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).

import re

ACTION_FIELD_RE = re.compile(r'(\w+)=((?:\[[^\]]*\])|(?:[^\s]+))')

def parse_action_line(line):
    """
    Parses a Lustre HSM action log line for core event attributes.
    """
    data = {}
    parts = ACTION_FIELD_RE.findall(line)
    for key, val in parts:
        if key == "idx":
            try:
                cat_idx, rec_idx = val.strip("[]").split("/")
                data["cat_idx"] = int(cat_idx)
                data["rec_idx"] = int(rec_idx)
            except (ValueError, IndexError):
                continue
        elif key in ("action", "fid", "status"):
            data[key] = val.strip("[]")
        elif val.startswith('[') and val.endswith(']'):
            inner = val[1:-1]
            inner_parts = re.findall(r'(\w+)=([^\s\[\]]+)', inner)
            for ikey, ival in inner_parts:
                if ikey == "idx":
                    # Only overwrite idx if it's not already set
                    if "cat_idx" not in data:
                        try:
                            cat_idx, rec_idx = ival.split("/")
                            data["cat_idx"] = int(cat_idx)
                            data["rec_idx"] = int(rec_idx)
                        except (ValueError, IndexError):
                            continue
                # Only set inner fields if they haven't been found at the top level
                elif ikey in ("action", "fid", "status") and ikey not in data:
                    data[ikey] = ival

    if "cat_idx" in data and "rec_idx" in data:
        return data
    return None
