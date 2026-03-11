#!/usr/bin/env python3
"""OCR scanned Minutes PDFs to text files."""
import os
import sys
from pathlib import Path

try:
    from pdf2image import convert_from_path
    import pytesseract
except ImportError:
    print("Install: pip3 install pdf2image pytesseract")
    sys.exit(1)

DOWNLOAD_DIR = Path(__file__).parent / "downloads"

# All dates that need OCR (no .txt file yet and PDF exists)
NEEDS_OCR = [
    "2015-11-18","2015-12-16",
    "2016-01-04","2016-02-10","2016-05-18","2016-06-01","2016-08-24","2016-10-05","2016-11-18","2016-12-21",
    "2017-01-18","2017-02-01","2017-03-22","2017-03-28","2017-04-05","2017-04-19","2017-05-03","2017-05-17",
    "2017-06-07","2017-07-05","2017-07-19","2017-08-23","2017-09-18","2017-09-21","2017-09-27","2017-10-11",
    "2017-11-08","2017-12-20","2018-03-07",
    # 2012/2013 ones are text-extractable but let's also OCR them for completeness
    "2012-03-26","2012-04-13","2012-07-16","2012-11-21","2013-08-21",
]

done = 0
total = len(NEEDS_OCR)

for d in NEEDS_OCR:
    pdf_path = DOWNLOAD_DIR / d / f"minutes_{d}.pdf"
    txt_path = DOWNLOAD_DIR / d / f"minutes_{d}.txt"
    
    if txt_path.exists() and txt_path.stat().st_size > 500:
        done += 1
        print(f"[{done}/{total}] {d}: already has OCR text ({txt_path.stat().st_size} bytes), skipping")
        continue
    
    if not pdf_path.exists():
        done += 1
        print(f"[{done}/{total}] {d}: NO PDF, skipping")
        continue
    
    try:
        images = convert_from_path(str(pdf_path), dpi=300)
        all_text = []
        for i, img in enumerate(images):
            page_text = pytesseract.image_to_string(img, lang='eng')
            all_text.append(page_text)
        
        full_text = '\n\n'.join(all_text)
        txt_path.write_text(full_text, encoding='utf-8')
        done += 1
        print(f"[{done}/{total}] {d}: OCR'd {len(images)} pages -> {len(full_text)} chars")
    except Exception as e:
        done += 1
        print(f"[{done}/{total}] {d}: ERROR - {e}")

print(f"\nDone! {done}/{total} processed")
