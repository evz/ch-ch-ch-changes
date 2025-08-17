import json
from collections import OrderedDict
from datetime import datetime
from itertools import groupby
from operator import itemgetter

from app.api import api
from flask import make_response, request
from helpers import groupedChanges


def dthandler(obj):
    """Handle datetime objects in JSON serialization."""
    return obj.isoformat() if isinstance(obj, datetime) else None


@api.route("/")
def listing():
    pagination_args = {}
    for arg in ["limit", "offset", "order_by", "sort_order"]:
        if request.args.get(arg):
            pagination_args[arg] = request.args[arg]

    changed_records, query, count = groupedChanges(**pagination_args)

    changed_records = [OrderedDict(zip(changed_records.keys(), r)) for r in changed_records]

    changed_records = sorted(changed_records, key=itemgetter("id"))
    record_groups = []
    for record_id, group in groupby(changed_records, key=itemgetter("id")):
        record_groups.append({record_id: list(group)})

    meta = {"total_count": count}
    meta.update(query)

    resp = {
        "status": "ok",
        "meta": meta,
        "records": record_groups,
    }

    response = make_response(json.dumps(resp, default=dthandler, sort_keys=False))

    response.headers["Content-Type"] = "application/json"
    return response
