# Vehicle Fleet Distributed Database — Project #89

> **Course**: Distributed Database Systems  
> **Topic**: Distributed Inheritance Handling: "Vehicle Fleet"  
> **Reference**: Özsu & Valduriez, *Principles of Distributed Database Systems* (4th Ed.)

---

## 📋 Project Overview

Implements **Distributed Inheritance Handling** for a Vehicle Fleet system across 3 independent sites:

| Concept | Implementation | Özsu & Valduriez Reference |
|---|---|---|
| **OID Management** | Anchored at base fragment (Site 0), reused by subclasses for join | Object Identity |
| **Complexity Handling** | `Vehicle → Truck`, `Vehicle → ElectricCar` hierarchy | The Object Model |
| **Network Awareness** | Measures per-site fetch time + rehydration overhead | Distributed Object Queries |
| **Serialization** | JSON roundtrip with schema version tracking | Object Serialization |
| **Garbage Collection** | Reference metadata hooks on all objects | Garbage Collection |
| **Schema Evolution** | Broadcast attribute additions + lazy migration | Schema Evolution |

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│              COORDINATOR (Site 0 / site0:5000)               │
│           Orchestrates Polymorphic Search & Joins            │
└──────────┬──────────────────────────┬────────────────────────┘
           │ HTTP/REST                │ HTTP/REST
  ┌────────▼───────┐        ┌────────▼────────┐
  │   Site 1       │        │   Site 2        │
  │   Truck        │        │   ElectricCar   │
  │   site1:5001   │        │   site2:5002    │
  └────────────────┘        └─────────────────┘
```

**Fragmentation**: Vertical — Site 0 holds `make/model/year/vin`, Sites 1 & 2 hold subclass-specific attributes. Fragments joined by OID during Polymorphic Search.

---

## 🚀 Quick Start

### Option A: Docker (Recommended — simulates real distributed environment)

```bash
# 1. Build & start all 3 site containers
docker compose up --build -d
```

### 2. Bơm Dữ Liệu Thực Tế (Seed Data)

> **Lưu ý:** Nếu chạy lại lệnh này nhiều lần, dữ liệu sẽ cộng dồn. Để reset trắng dữ liệu, hãy chạy `docker compose down -v` trước.

```bash
docker compose --profile seed run --rm seeder
# Hoặc chạy script nội bộ trên Windows:
# python setup/seed_data.py
```

### 3. Chạy Client (Tương tác với hệ thống)

```bash
pip install -r requirements.txt
python main.py
```

### 4. Demo site failure

```bash
# kill Site 1 then run option [7] in main.py
docker compose stop site1
python main.py    # -> option [7]
```

### 5. Bring Site 1 back

```bash
docker compose start site1
```


---

## 📁 Project Structure

```
vehicle-fleet-distributed/
├── src/
│   ├── oid_manager.py     # OID generation & registry (Object Identity)
│   ├── config.py          # Site topology — Docker-aware via env vars
│   ├── models.py          # Vehicle / Truck / ElectricCar + serialization
│   ├── site_server.py     # Flask HTTP server for each site
│   └── coordinator.py     # Polymorphic search & schema evolution
├── setup/
│   └── seed_data.py       # Populate all sites with sample data
├── data/                  # Local JSON storage (auto-created)
├── main.py                # Interactive CLI demo
├── Dockerfile             # Single site server image
├── docker-compose.yml     # 3-site orchestration
└── requirements.txt
```

---

## 🐳 Why Docker?

| Aspect | Without Docker | With Docker |
|---|---|---|
| **Site isolation** | Same OS process space | Separate network namespace per container |
| **Failure simulation** | Kill process (disrupts other sites) | `docker compose stop site1` — clean, instant |
| **Communication** | All via localhost | Via Docker DNS: `site0`, `site1`, `site2` |
| **Data isolation** | Shared `data/` folder | Separate named volumes per site |
| **Reproducibility** | Depends on local Python env | Identical environment everywhere |

Docker makes each "site" truly autonomous (Site Autonomy), not just a different port on the same machine.

---

## 🎯 Key Demonstrations (in `main.py`)

| Option | Demo | Theory |
|---|---|---|
| [1] | Ping all sites — see which are online | Site Autonomy |
| [2] | Polymorphic Search all vehicles | Distributed Object Queries |
| [3] | Filter by make (e.g., Tesla) | Distributed Query |
| [4] | Filter by year range | Distributed Query |
| [5] | Rehydration cost: Site-0-only vs. all sites | Distributed Object Queries |
| [6] | Benchmark scaling performance | Parallel Execution & Network Scaling |
| [7] | Add attribute to Vehicle → propagate all sites | Eventual Schema Evolution |
| [8] | OID manager stats per site | Object Identity |
| [9] | Distributed Query Planner (Semi-Join) | Distributed Query Optimization |

---

## 📊 Dataset

| Site | Container | Class | Records |
|---|---|---|---|
| Site 0 | `vf_site0` | Vehicle | ~500 base records |
| Site 1 | `vf_site1` | Truck | ~150 Truck fragment records |
| Site 2 | `vf_site2` | ElectricCar | ~200 ElectricCar fragment records |

Brands: Volvo, Mercedes, MAN, Scania, DAF, IVECO (Trucks) + Tesla, BYD, Hyundai, Rivian (EVs) + Toyota, Ford, VW, Renault (base only)

---

## 📚 Theory References

- The Object Model: classes, attributes, inheritance  
- Object Identity in Distributed Systems  
- Distributed Object Queries & Serialization  
- Schema Evolution in Distributed OODBs  
- Garbage Collection: Reference Counting  
- Vertical Fragmentation  
- Parallel Query Execution  
- Availability vs. Consistency  
