def plan_to_mermaid(plan, stats):
    excluded_nodes = []
    builder = ""

    node_stats = {x["identity"]: x for x in stats}

    for nid, node in plan.nodes(True):
        if node.is_not_explained:
            excluded_nodes.append(nid)
            continue
        builder += f"  {node.to_mermaid(node_stats.get(node.identity), nid)}\n"
        node_stats[nid] = node_stats.pop(node.identity, None)
    builder += "\n"
    for s, t, r in plan.edges():
        if t in excluded_nodes:
            continue
        stats = node_stats.get(s)
        join_leg = f"**{r.upper()}**<br />" if r else ""
        builder += f'  NODE_{s} -- "{join_leg} {stats.get("records_out"):,} rows<br />{stats.get("bytes_out"):,} bytes" --> NODE_{t}\n'

    return "flowchart BT\n\n" + builder
