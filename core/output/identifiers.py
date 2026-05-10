def assign_output_identifiers(gdf, start_id):
    identified = gdf.copy()
    if "acm_id" not in identified.columns:
        identified["acm_id"] = range(start_id, start_id + len(identified))
    identified["fid"] = identified["acm_id"]
    return identified
