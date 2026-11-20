# TFEX Real-time 1-Minute Arbitrage Radar (`tfex-arb-opp`)

ระบบสแกนและแจ้งเตือนโอกาส **Arbitrage** ในตลาด **TFEX SET50 Index Futures & Options** แบบเรียลไทม์ **ราย 1 นาที (1-Minute High-Frequency Sync)** จากข้อมูล Intraday Snapshot ใน Turso Cloud SQLite พร้อมส่งสัญญาณเข้า Telegram ทันทีที่พบโอกาส และสรุปภาพรวมปิดเซสชันวันละ 2 รอบ

---

## 🎯 3 โมดูลสแกนโอกาส Arbitrage

1. **Put-Call Parity Arbitrage (Conversion & Reversal):**
   - คำนวณความสัมพันธ์ตามแบบจำลอง **Black-76 Futures Options**:
     $$C - P = (F - K) \cdot e^{-rT}$$
     โดยใช้อัตราดอกเบี้ย THOR จากธนาคารแห่งประเทศไทย (`eod_thor_rates`)
   - หักค่าคอมมิชชั่น 3 ขา **0.60 จุด** (40 บาท/สัญญา/ขา = 120 บาท/ชุด)
   - ติดป้ายกำกับสภาพคล่อง: `[Liquid: Spot±30]` และ `[Deep Strike: ถือจนหมดอายุ]`
2. **Vertical Spread & Monotonicity Violations:**
   - ตรวจสอบความผิดปกติของลำดับราคาตาม Strike (เช่น Call Strike ต่ำ ราคา Ask ถูกกว่า Call Strike สูง ราคา Bid)
   - หักค่าคอมมิชชั่น 2 ขา **0.40 จุด** (80 บาท/ชุด)
3. **Calendar Spread Spikes (Front vs Next Quarter Futures):**
   - คำนวณส่วนต่างราคา Bid/Ask ข้ามซีรีส์ เทียบกับค่าเฉลี่ยเคลื่อนที่ในวันนั้น (`Rolling Mean ± 2 SD`)
   - ป้องกัน False Signal จากผลกระทบเงินปันผล XD ข้ามไตรมาส

---

## 🛡️ กฎการคำนวณตามการ Execution จริง (Strict Rules)

- **Immediate Execution (เคาะซื้อขายทันที):** ซื้อที่ราคา `Ask` (ต้องมี `ask > 0` และ `volume > 0`) และขายที่ราคา `Bid` (ต้องมี `bid > 0` และ `volume > 0`) เท่านั้น ห้ามใช้ราคา `Last` หรือ `Mid` เด็ดขาด
- **Executable Sets (`max_sets`):** คำนวณจาก `min(volume)` ของทุกขาที่เกี่ยวข้องในนาทีเดียวกัน (`captured_at_bkk` ตรงกันเป๊ะ)
- **1-Minute Sync Mechanism:** ในแต่ละนาที จะรอจนถึงวินาทีที่ `:06` หรือ Poll เช็ก `MAX(captured_at_bkk)` ใน `futures_snapshots` และ `options_snapshots` ทุก 3 วินาที เพื่อให้มั่นใจว่าข้อมูลทั้งสองตารางบันทึกเสร็จสมบูรณ์แล้วจึงดึงมาประมวลผลทันที
- **Smart Anti-Spam Cooldown (15 นาที):** เมื่อพบโอกาสทำกำไรสุทธิ (> 0 จุด) จะแจ้งเตือนทันที (`🚨 [LIVE 1-MIN ARB] โอกาสเคาะทำกำไรทันที!`) และเก็บ Cache ไว้ 15 นาที หากคู่เดิมยังคงอยู่จะไม่สแปมซ้ำ ยกเว้นกรณีที่กำไรสุทธิกระโดดเพิ่มขึ้นอย่างน้อย `+0.20 จุด` หรือเป็น Strike/กลยุทธ์ใหม่ จะยิงแจ้งเตือนทันที
- **Strict Validation on Session Input:** `--session` เป็น **required** (ไม่มี default) — ไม่ระบุหรือระบุผิดจะจบด้วย Error ทันที (No Silent Fallback) เดิมค่า default เคยเป็น `MIDDAY` ทำให้ลืม flag แล้วระบบรันผิดโหมดเงียบ ๆ

### 🛡️ Pre-Alert REST Verification Gate (fail-closed)

ก่อนยิง alert ทุกครั้ง ระบบจะ re-quote ทุกขาของอันดับต้น ๆ (สูงสุด `MAX_REST_VERIFIED_OPPS = 3`) ผ่าน Settrade MarketData REST แล้วคำนวณ net profit ใหม่ ถ้าขาไหน depth < 1 หรือ net ≤ 0 จะถูกตัดทิ้ง

- **ถ้า Settrade ใช้งานไม่ได้** (secret หมดอายุ / เน็ตหลุด / ไม่ได้ตั้งค่า) ระบบจะ **ระงับ alert** (fail-closed) ไม่ใช่ปล่อยผ่าน
- ต้องการแจ้งเตือนด้วยราคาใน DB อยู่ ให้ใส่ `--allow-unverified` — alert จะติดป้าย `⚠️ DB-only (ยังไม่ verified)` ทุกครั้ง
- อันดับ #2 ขึ้นไปที่ยังไม่ผ่าน REST จะติดป้าย DB-only เช่นกัน ไม่แสดง `REST Verified ✅` ปลอม
- Quote ที่ไม่ขยับเกิน **120 วินาที** จะถูกตัดออกจาก live scan (`QUOTE_STALENESS_LIVE_SEC`) ส่วน backfill สรุปปิดรอบใช้ 900 วินาที

---

## 🧩 โค้ดที่ใช้ร่วมกับ repo อื่น (tfex-common)

ปฏิทิน SET / expiry, THOR unit handling, Turso client และ Telegram dispatch ไม่ได้อยู่ในไฟล์นี้แล้ว — import จาก [`tfex-common`](https://github.com/met37818-stack/tfex-common) (ปักหมุด tag ใน `requirements.txt`) เพื่อให้ทั้ง 4 repo ใช้โค้ดชุดเดียวกัน

> `BANGKOK_TZ` ยังเป็น `pytz` ในไฟล์นี้โดยเจตนา เพราะโค้ดใช้ `.localize()` ซึ่ง tzinfo แบบ fixed-offset ของ package ไม่มีให้

## ⏰ รอบการทำงานอัตโนมัติ (Triggered via cron-job.org)

ปลุก Persistent Worker ผ่าน GitHub Actions REST API (`workflow_dispatch`) วันละ 2 รอบล่วงหน้าก่อนตลาดเปิด 5 นาที:

- **รอบเช้า (`MORNING_WORKER`):** ปลุกที่ **09:40 น.** (สแตนด์บายรอจนถึง 09:45 แล้ววนลูปสแกนจนถึง 12:35 น. พร้อมส่งสรุปปิดรอบเช้า 165 แท่ง)
- **รอบบ่าย (`AFTERNOON_WORKER`):** ปลุกที่ **13:40 น.** (สแตนด์บายรอจนถึง 13:45 แล้ววนลูปสแกนจนถึง 17:05 น. พร้อมส่งสรุปภาพรวมทั้งวัน 190 แท่งบ่าย รวมเต็มวัน 355 แท่ง)

### Exact JSON Payload สำหรับตั้งค่าใน `cron-job.org`:
- Endpoint: `POST https://api.github.com/repos/{owner}/tfex-arb-opp/actions/workflows/arb_radar.yml/dispatches`

**1) รอบเช้า (09:40 น. BKK):**
```json
{
  "ref": "main",
  "inputs": {
    "session": "MORNING_WORKER"
  }
}
```

**2) รอบบ่าย (13:40 น. BKK):**
```json
{
  "ref": "main",
  "inputs": {
    "session": "AFTERNOON_WORKER"
  }
}
```

*(หมายเหตุ: โหมด Single-Shot สำหรับทดสอบด่วนยังคงรองรับ `inputs.session`: `MIDDAY` และ `EVENING` เช่นเดิม)*

---

## 🚀 การติดตั้งและใช้งาน (Local Execution)

```bash
# ติดตั้ง dependencies (pin แล้ว)
pip install -r requirements.txt

# รัน Real-time 1-Minute Worker รอบเช้า
python arb_scanner.py --session MORNING_WORKER

# รัน Real-time 1-Minute Worker รอบบ่าย
python arb_scanner.py --session AFTERNOON_WORKER

# รันแบบ Single-shot สแกนเฉพาะรอบพักเที่ยง
python arb_scanner.py --session MIDDAY

# รันแบบ Single-shot สแกนสรุปทั้งวัน
python arb_scanner.py --session EVENING

# ยอมรับ alert ที่ไม่ผ่าน REST verification (จะติดป้าย DB-only)
python arb_scanner.py --session AFTERNOON_WORKER --allow-unverified
```

### 🧪 การรันชุดทดสอบ

```bash
pip install -r requirements-dev.txt   # requirements.txt + pytest (pin แล้ว)
python -m pytest -q                    # คาดหวัง 39 passed
```

`--session` เป็น required — ถ้าไม่ใส่จะได้ HTTP 400/argparse error ทันที ไม่ fallback ไป MIDDAY
