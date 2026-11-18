# TFEX Intraday Arbitrage Radar (`tfex-arb-opp`)

ระบบสแกนและแจ้งเตือนโอกาส **Arbitrage** ในตลาด **TFEX SET50 Index Futures & Options** แบบเรียลไทม์ระหว่างวัน จากข้อมูล Intraday Snapshot ใน Turso Cloud SQLite พร้อมส่งรายงานเข้า Telegram วันละ 2 รอบ

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

- **Strict Bid/Ask:** ซื้อที่ราคา `Ask` และขายที่ราคา `Bid` เท่านั้น
- **Executable Sets (`max_sets`):** คำนวณจาก `min(volume)` ของทุกขาที่เกี่ยวข้องในนาทีเดียวกัน (`captured_at_bkk` ตรงกันเป๊ะ)
- **Near-Miss Evaluation:** หากไม่พบโอกาสที่ได้กำไรสุทธิ (> 0 จุด) ระบบจะรายงานคู่ที่เข้าใกล้จุดคุ้มทุนที่สุด
- **Strict Validation on Session Input:** บังคับตรวจสอบค่า `--session` (MIDDAY, EVENING, MORNING_WORKER, AFTERNOON_WORKER) หากระบุผิดหรือไม่ระบุจะตัดจบการทำงานด้วย Error ทันที (No Silent Fallback)

---

## ⏰ รอบการแจ้งเตือนอัตโนมัติ (Triggered via cron-job.org)

ปลุกการทำงานผ่าน GitHub Actions REST API (`workflow_dispatch`) วันละ 2 รอบ:

- **รอบพักเที่ยง (MIDDAY):** 12:38 น. (เวลาไทย) — สแกนช่วงเช้า 09:45–12:35 น.
- **รอบปิดตลาดเย็น (EVENING):** 17:08 น. (เวลาไทย) — สแกนทั้งวัน 09:45–17:05 น.

### ตัวอย่าง JSON Payload สำหรับยิง GitHub REST API:
- Endpoint: `POST https://api.github.com/repos/{owner}/tfex-arb-opp/actions/workflows/arb_radar.yml/dispatches`

**รอบ MIDDAY (12:38 น.):**
```json
{
  "ref": "main",
  "inputs": {
    "session": "MIDDAY"
  }
}
```

**รอบ EVENING (17:08 น.):**
```json
{
  "ref": "main",
  "inputs": {
    "session": "EVENING"
  }
}
```

---

## 🚀 การติดตั้งและใช้งาน (Local Execution)

```bash
# ติดตั้ง dependencies
pip install -r requirements.txt

# รันจริงกับฐานข้อมูล Turso รอบพักเที่ยง
python arb_scanner.py --session MIDDAY

# รันจริงกับฐานข้อมูล Turso รอบปิดตลาดเย็น
python arb_scanner.py --session EVENING
```
