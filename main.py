"""
Schedule running an extraction Add-On on a project of documents on a
schedule.

Modeled on MuckRock's OCR Scheduler add-on:
https://github.com/MuckRock/ocr-scheduler
"""
from itertools import islice

from documentcloud.addon import AddOn

# TODO: fill in real run IDs as confirmed via
# GET https://api.www.documentcloud.org/api/addons/?query=<name>
EXTRACTOR_RUN_IDS = {
    "azure": 875,  # MuckRock/azure-table-extractor
    "textract": 877,  # MuckRock/textract-table-extractor-add-on
}

# Confirmed via GET https://api.www.documentcloud.org/api/addons/?query=<name>
# (see each add-on's "parameters.properties" for the live schema). Only
# include keys that schema actually defines -- neither Azure Table
# Extractor nor Textract Table Extractor has a "to_tag" property, for
# instance, so it's left out rather than sent as an unrecognized extra
# field.
#
# NOTE: both extractors' start_page AND end_page default to 1 -- if you
# don't set these, they silently only process page 1 of every document.
# end_page is filled in per-document below using the document's actual
# page count, since it varies doc to doc.
EXTRACTOR_PARAMS = {
    "azure": {
        "start_page": 1,
        "output_format": "json",
    },
    "textract": {
        "start_page": 1,
        "output_format": "csv",  # Textract's schema has no "json" option
    },
}

# Extractors whose config takes a start_page/end_page range. For these,
# end_page is set per-document from the document's own page count
# rather than a single shared value across the batch.
PAGE_RANGE_EXTRACTORS = {"azure", "textract"}

# Tag written onto each document as it's queued, so re-runs of this
# scheduler skip documents already sent to a given extractor. This is
# set by the scheduler itself rather than relying on each downstream
# Add-On to write back its own completion field, since that's not
# something we can confirm from the outside for all three extractors.
def dedup_field(extractor):
    return f"data_extractor_{extractor}"


class ExtractorScheduler(AddOn):
    """Dispatch a batch of documents in a project to a chosen extractor
    Add-On, skipping documents already queued for that extractor."""

    def main(self):
        extractor = self.data.get("extractor")
        batch_size = int(self.data.get("batch_size", 25))

        run_id = EXTRACTOR_RUN_IDS.get(extractor)
        if run_id is None:
            self.set_message(
                f"No run_id configured for extractor '{extractor}' -- "
                "fill in EXTRACTOR_RUN_IDS in main.py."
            )
            return

        field = dedup_field(extractor)

        if self.query:
            # Scheduled runs: re-run the same saved search each time,
            # so newly-matching documents get picked up automatically.
            documents = self.client.documents.search(
                f"({self.query}) -{field}:* +status:success"
            )
            batch = list(islice(documents, 0, batch_size))
        else:
            # One-off runs against a fixed document selection.
            batch = [
                doc
                for doc in islice(self.get_documents(), 0, batch_size)
                if not doc.data.get(field)
            ]

        if not batch:
            self.set_message(f"No documents left to queue for {extractor}.")
            return

        base_params = EXTRACTOR_PARAMS.get(extractor, {})
        needs_page_range = extractor in PAGE_RANGE_EXTRACTORS

        queued = 0
        for i, doc in enumerate(batch):
            self.set_progress(int((i / len(batch)) * 100))

            parameters = dict(base_params)
            if needs_page_range:
                # Each document has its own page count, so end_page has
                # to be set per document rather than once for the batch.
                parameters["end_page"] = doc.page_count

            self.client.post(
                "addon_runs/",
                json={
                    "addon": run_id,
                    "parameters": parameters,
                    "documents": [doc.id],
                    "dismissed": True,
                },
            )

            doc.data[field] = "queued"
            doc.put()
            queued += 1

        self.set_progress(100)
        self.set_message(f"Queued {queued} documents for {extractor}.")


if __name__ == "__main__":
    ExtractorScheduler().main()
