# IE212 - Alibaba Microservices

Phân tích đồ thị lời gọi, nhận diện dịch vụ trọng yếu và dự báo độ trễ microservices trên dữ liệu Alibaba bằng Data Lakehouse.

**Nhóm:** Huy và Vy. **Báo cáo:** 17/12/2026.

## Trạng thái

Repo đang ở bước tạo cấu trúc. Chưa có pipeline, môi trường chạy hoàn chỉnh, dữ liệu Alibaba, mô hình hay kết quả thực nghiệm. Compose, requirements và YAML là khung để triển khai; chưa có lệnh chạy pipeline. Không coi skeleton là hoàn thành các task thực nghiệm.

## Cấu trúc thư mục

```text
ie212-microservices/
├── README.md
├── agents_ie212.md
├── .gitignore
├── .env.example
├── docker-compose.yml
├── requirements.txt
├── configs/
│   ├── data.yaml
│   ├── lakehouse.yaml
│   ├── features.yaml
│   ├── experiments.yaml
│   └── benchmarks.yaml
├── src/ie212/
│   ├── ingestion/
│   ├── quality/
│   ├── silver/
│   ├── gold/
│   ├── graphs/
│   ├── features/
│   ├── models/
│   ├── evaluation/
│   └── benchmarks/
├── scripts/
├── tests/
├── notebooks/
├── dashboard/
├── artifacts/
│   ├── manifest/
│   ├── models/
│   ├── predictions/
│   └── benchmarks/
├── reports/
│   ├── data_quality/
│   ├── figures/
│   └── tables/
├── docs/
│   ├── data_dictionary.md
│   ├── decisions.md
│   ├── resources.md
│   ├── sampling_plan.md
│   ├── split_cohort.md
│   ├── benchmark_design.md
│   └── main.tex                 [nhóm bổ sung file có sẵn; chưa tạo]
└── data/
    ├── README.md
    ├── raw/
    └── sample/
```

Các thư mục rỗng giữ bằng `.gitkeep`, package Python có `__init__.py`. Cấu trúc và vai trò các thư mục được mô tả trong `agents_ie212.md`.

## Tài liệu và báo cáo LaTeX

Dùng một thư mục `docs/` phẳng. Nhóm đưa file LaTeX có sẵn vào `docs/main.tex`; repo chưa tạo file này. Không dùng `docs/report/` hoặc tách chương. Nếu template nhóm có dependency riêng, giữ đúng dependency thật; không tự gộp làm hỏng template. Hình và bảng phân tích để trong `reports/figures/`, `reports/tables/`; tham chiếu từ `docs/` qua `../reports/figures/` và `../reports/tables/`. Lệnh biên dịch sẽ ghi sau khi nhóm đưa template vào và xác minh engine.

## Phạm vi và cách làm việc

Đọc [metadata gốc](D:/IE212/Project/context/metadata.txt) và [agents_ie212.md](agents_ie212.md) trước khi triển khai. Mục tiêu gồm RQ1 đồ thị, RQ2 mean RPC RT phút kế tiếp với A/B/C và RQ3 benchmark Spark/DuckDB/Pandas. P95/cảnh báo là mở rộng có điều kiện.

Code dùng chung một repo: tạo nhánh `feature/<task>`, mở PR và để người còn lại review trước khi merge. Không đưa raw data, secrets hoặc artifacts lớn vào Git. Kế hoạch Excel của nhóm được quản lý ngoài repo, trong thư mục context của workspace.

## Nguồn metadata duy nhất

Luôn đọc `D:\IE212\Project\context\metadata.txt`. Không lưu bản sao metadata trong repo. Thư mục context nằm ngoài repo và không được GitHub tự đồng bộ; thành viên dùng máy khác cần có nguồn metadata tương ứng trước khi làm việc. Hai file kế hoạch/cấu trúc Excel được giữ trong context, không nằm trong docs. Hai thư mục output/outputs cũ đã được xóa.
