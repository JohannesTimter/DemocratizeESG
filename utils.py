import math
import time
from io import BytesIO
from pathlib import Path
from typing import Any

import httpx
import pypdf
from pypdf import PdfWriter

from ChainOfAgents import client, max_context
from CompanyReportFile import CompanyReportFile


def split_upload_pdf(doc: CompanyReportFile):
    n_parts = calculate_required_parts(doc)
    uploaded_docs = {}
    pdf_stream = BytesIO(doc.file_value)
    pdf_reader = pypdf.PdfReader(pdf_stream)
    pdf_chunk_bytes_io = BytesIO()
    total_pages = len(pdf_reader.pages)
    output_dir = Path("split_pdfs")
    output_dir.mkdir(exist_ok=True)
    base_chunk_size = total_pages // n_parts

    start_index = 0
    #Loop through and create the first n-1 chunks
    for i in range(n_parts - 1):
        end_index = start_index + base_chunk_size
        pdf_writer = pypdf.PdfWriter()

        start = time.time()
        for page_num in range(start_index, end_index):
            pdf_writer.add_page(pdf_reader.pages[page_num])
        end = time.time()
        print(f"time to add pages file: {int(end - start)}")

        output_filename = f"{doc.company_name}-{doc.period}-{doc.topic.name}-{doc.counter}_chunk_{i + 1}_pages_{start_index + 1}_{end_index}"
        write_chunk_to_file(output_dir, output_filename, pdf_writer)

        pdf_writer.write(pdf_chunk_bytes_io)
        uploaded_docs[output_filename] = upload_chunk(pdf_chunk_bytes_io)

        print(f"Created '{output_filename}' with {end_index - start_index} pages. Uploaded with id {uploaded_docs[output_filename].name}")

        #Set the start index for the NEXT chunk (this creates the overlap)
        start_index = end_index - 1

    # 4. Create the final chunk with all remaining pages
    if start_index < total_pages:
        final_writer = pypdf.PdfWriter()
        # The last chunk goes from the last start_index all the way to the end
        for page_num in range(start_index, total_pages):
            final_writer.add_page(pdf_reader.pages[page_num])

        output_filename = write_final_chunk_to_file(doc, final_writer, n_parts, output_dir, output_filename,
                                                    start_index, total_pages)

        final_writer.write(pdf_chunk_bytes_io)
        uploaded_docs[output_filename] = upload_chunk(pdf_chunk_bytes_io)

        print(f"Created '{output_filename}' with {total_pages - start_index} pages. Uploaded with id {uploaded_docs[output_filename].name}")

    return uploaded_docs


def calculate_required_parts(doc: CompanyReportFile) -> int:
    source_title = f"{doc.company_name}_{doc.period}_{doc.topic.name}_{doc.counter}"

    uploaded_pdf = client.files.upload(
        file=BytesIO(doc.file_value),
        config=dict(mime_type="application/pdf")
    )
    token_count_response = client.models.count_tokens(
        model="gemini-2.5-flash", contents=["Tell me about this file", uploaded_pdf]
    )
    print(f"Total tokens: {token_count_response.total_tokens}")

    print(f"{source_title}, {token_count_response.total_tokens}")
    n_parts = math.ceil(token_count_response.total_tokens / max_context)
    print(f"parts_required: {n_parts}")
    return n_parts


def write_chunk_to_file(output_dir: Path, output_filename: str, pdf_writer: PdfWriter):
    start = time.time()
    output_path = output_dir / (output_filename + '.pdf')
    with open(output_path, "wb") as output_file:
        pdf_writer.write(output_file)
    end = time.time()
    print(f"time to write file: {int(end - start)}")


def upload_chunk(pdf_chunk_bytes_io):
    uploaded_doc = None
    start = time.time()

    retries = 0
    max_retries: int = 5
    initial_delay: float = 1.0
    backoff_factor: float = 2.0

    delay = initial_delay
    pdf_chunk_bytes_io.seek(0)
    try:
        uploaded_doc = client.files.upload(
            file=pdf_chunk_bytes_io,
            config=dict(
                mime_type="application/pdf")
        )
    except httpx.RemoteProtocolError as e:
        retries += 1
        if retries >= max_retries:
            print(f"Upload failed after {retries} retries.")
            raise e
        print(f"Upload failed with RemoteProtocolError. Retrying in {delay} seconds.")
        time.sleep(delay)
        delay *= backoff_factor

    end = time.time()
    print(f"time to upload pdf: {int(end - start)}")

    return uploaded_doc


def write_final_chunk_to_file(doc: CompanyReportFile, final_writer: PdfWriter, n_parts: int, output_dir: Path,
                              output_filename: str, start_index: int | Any, total_pages: int) -> str:
    output_filename = f"{doc.company_name}-{doc.period}-{doc.topic.name}-{doc.counter}chunk_{n_parts}_pages_{start_index + 1}_{total_pages}"
    output_path = output_dir / (output_filename + '.pdf')
    with open(output_path, "wb") as output_file:
        final_writer.write(output_file)
    return output_filename
