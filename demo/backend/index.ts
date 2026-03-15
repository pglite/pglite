import { PGLite } from "@pglite/core";
import { NodeFSAdapter } from "@pglite/core/node-fs";

// Initialize the engine with a local file path
const db = new PGLite("app.db", {
  adapter: new NodeFSAdapter(),
});

const SQL = `
-- Database Schema for ZenMaster Management System
-- Thể hiện kiến trúc hệ thống quản lý giáo dục chuyên nghiệp, tích hợp CRM, Tài chính và Học vụ.

-- 1. Bảng người dùng (Bắt buộc theo yêu cầu Framework Auth)
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    username TEXT UNIQUE NOT NULL,
    full_name TEXT,
    phone TEXT,
    email TEXT,
    password TEXT NOT NULL,
    zalo_id TEXT,
    role TEXT NOT NULL CHECK (role IN ('admin', 'staff', 'teacher', 'parent', 'zalo', 'user')),
    avatar_url TEXT,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'banned')),
    base_salary NUMERIC(15, 2) DEFAULT 0,
    teaching_rate NUMERIC(15, 2) DEFAULT 500000,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE users IS '{ "label": "Người dùng", "description": "Bảng lưu trữ thông tin tài khoản đăng nhập cho toàn bộ hệ thống (Admin, Giáo viên, Phụ huynh)" }';
COMMENT ON COLUMN users.id IS '{ "label": "ID", "type": "number", "required": true, "description": "Mã định danh duy nhất", "visible": false }';
COMMENT ON COLUMN users.username IS '{ "label": "Tên đăng nhập", "type": "short_text", "required": true, "description": "Tên dùng để đăng nhập hệ thống", "visible": true }';
COMMENT ON COLUMN users.full_name IS '{ "label": "Họ và tên", "type": "short_text", "required": true, "description": "Họ và tên đầy đủ", "visible": true }';
COMMENT ON COLUMN users.phone IS '{ "label": "Số điện thoại", "type": "short_text", "required": false, "description": "Số điện thoại liên hệ", "visible": true }';
COMMENT ON COLUMN users.email IS '{ "label": "Email", "type": "email", "required": false, "description": "Địa chỉ email liên hệ", "visible": true }';
COMMENT ON COLUMN users.password IS '{ "label": "Mật khẩu", "type": "password", "required": true, "description": "Mật khẩu đã được mã hóa", "visible": false }';
COMMENT ON COLUMN users.zalo_id IS '{ "label": "Zalo ID", "type": "short_text", "required": false, "description": "ID liên kết với tài khoản Zalo", "visible": true }';
COMMENT ON COLUMN users.role IS '{ "label": "Vai trò", "type": "select", "required": true, "description": "Phân quyền người dùng: admin, staff, teacher, parent, zalo, user", "enums": [{"label": "Admin", "value": "admin"}, {"label": "Nhân viên", "value": "staff"}, {"label": "Giáo viên", "value": "teacher"}, {"label": "Phụ huynh", "value": "parent"}] }';
COMMENT ON COLUMN users.avatar_url IS '{ "label": "Ảnh đại diện", "type": "short_text", "required": false, "description": "URL hình ảnh cá nhân", "visible": true }';
COMMENT ON COLUMN users.status IS '{ "label": "Trạng thái", "type": "select", "required": true, "description": "Trạng thái tài khoản: active, inactive, banned", "enums": [{"label": "Hoạt động", "value": "active"}, {"label": "Khóa", "value": "inactive"}] }';
COMMENT ON COLUMN users.base_salary IS '{ "label": "Lương cơ bản", "type": "decimal", "required": false, "description": "Mức lương cơ bản hàng tháng cho nhân viên/giáo viên", "visible": true }';
COMMENT ON COLUMN users.teaching_rate IS '{ "label": "Thù lao dạy", "type": "decimal", "required": false, "description": "Mức thù lao cho mỗi buổi dạy (mặc định 500k)", "visible": true }';
COMMENT ON COLUMN users.created_at IS '{ "label": "Ngày tạo", "type": "datetime", "required": false, "description": "Thời điểm tài khoản được tạo", "visible": true }';
COMMENT ON COLUMN users.updated_at IS '{ "label": "Ngày cập nhật", "type": "datetime", "required": false, "description": "Thời điểm cập nhật cuối cùng", "visible": false }';
COMMENT ON COLUMN users.deleted_at IS '{ "label": "Ngày xóa", "type": "datetime", "required": false, "description": "Thời điểm xóa mềm", "visible": false }';

-- 1.1 Vai trò & Quyền hạn (RBAC)
CREATE TABLE IF NOT EXISTS roles (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE roles IS '{ "label": "Vai trò", "description": "Định nghĩa các nhóm quyền trong hệ thống" }';
COMMENT ON COLUMN roles.id IS '{ "label": "ID", "type": "number", "required": true, "description": "Mã vai trò", "visible": false }';
COMMENT ON COLUMN roles.name IS '{ "label": "Tên vai trò", "type": "short_text", "required": true, "description": "Tên hiển thị của vai trò", "visible": true }';
COMMENT ON COLUMN roles.description IS '{ "label": "Mô tả", "type": "long_text", "required": false, "description": "Mô tả nhiệm vụ của vai trò", "visible": true }';
COMMENT ON COLUMN roles.created_at IS '{ "label": "Ngày tạo", "type": "datetime", "required": false, "description": "Ngày tạo", "visible": true }';
COMMENT ON COLUMN roles.updated_at IS '{ "label": "Ngày cập nhật", "type": "datetime", "required": false, "description": "Ngày cập nhật", "visible": false }';
COMMENT ON COLUMN roles.deleted_at IS '{ "label": "Ngày xóa", "type": "datetime", "required": false, "description": "Ngày xóa", "visible": false }';

CREATE TABLE IF NOT EXISTS permissions (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    module TEXT NOT NULL,
    key TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE permissions IS '{ "label": "Quyền hạn", "description": "Danh mục các hành động được phép trong hệ thống" }';
COMMENT ON COLUMN permissions.module IS '{ "label": "Module", "type": "short_text", "required": true, "description": "Tên module (CRM, Tài chính, Học vụ...)", "visible": true }';
COMMENT ON COLUMN permissions.key IS '{ "label": "Mã quyền", "type": "short_text", "required": true, "description": "Mã định danh quyền (ví dụ: crm.lead.view)", "visible": true }';
COMMENT ON COLUMN permissions.label IS '{ "label": "Tên quyền", "type": "short_text", "required": true, "description": "Tên hiển thị của quyền", "visible": true }';
COMMENT ON COLUMN permissions.created_at IS '{ "label": "Ngày tạo", "type": "datetime", "required": false, "description": "Ngày tạo", "visible": true }';
COMMENT ON COLUMN permissions.updated_at IS '{ "label": "Ngày cập nhật", "type": "datetime", "required": false, "description": "Ngày cập nhật", "visible": false }';
COMMENT ON COLUMN permissions.deleted_at IS '{ "label": "Ngày xóa", "type": "datetime", "required": false, "description": "Ngày xóa", "visible": false }';

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id INTEGER REFERENCES roles(id),
    permission_id INTEGER REFERENCES permissions(id),
    PRIMARY KEY (role_id, permission_id)
);

COMMENT ON TABLE role_permissions IS '{ "label": "Phân quyền vai trò", "description": "Bảng trung gian liên kết vai trò và quyền hạn" }';

-- 2. Khóa học
CREATE TABLE IF NOT EXISTS courses (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    title TEXT NOT NULL,
    description TEXT,
    base_price NUMERIC(15, 2) DEFAULT 0,
    duration_weeks INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE courses IS '{ "label": "Khóa học", "description": "Danh mục các chương trình đào tạo của trung tâm" }';
COMMENT ON COLUMN courses.title IS '{ "label": "Tên khóa học", "type": "short_text", "required": true, "description": "Tên hiển thị của khóa học", "visible": true }';
COMMENT ON COLUMN courses.base_price IS '{ "label": "Học phí gốc", "type": "decimal", "required": true, "description": "Số tiền học phí niêm yết", "visible": true }';

-- 3. Lớp học
CREATE TABLE IF NOT EXISTS classes (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    course_id INTEGER REFERENCES courses(id),
    teacher_id INTEGER REFERENCES users(id),
    name TEXT NOT NULL,
    room TEXT,
    max_students INTEGER DEFAULT 15,
    start_date DATE,
    end_date DATE,
    status TEXT DEFAULT 'opening' CHECK (status IN ('opening', 'active', 'closed', 'suspended')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE classes IS '{ "label": "Lớp học", "description": "Thông tin chi tiết về các lớp học đang hoặc sắp diễn ra" }';
COMMENT ON COLUMN classes.teacher_id IS '{ "label": "Giáo viên", "type": "foreign_key", "table": "users", "column": "id", "required": true, "description": "Giáo viên phụ trách lớp học" }';

-- 4. Học viên
CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    user_id INTEGER REFERENCES users(id), -- Nếu học viên có tài khoản tự đăng nhập
    parent_id INTEGER REFERENCES users(id), -- Liên kết với tài khoản phụ huynh
    full_name TEXT NOT NULL,
    student_code TEXT UNIQUE NOT NULL,
    date_of_birth DATE,
    wallet_balance NUMERIC(15, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE students IS '{ "label": "Học viên", "description": "Hồ sơ học viên và số dư ví nội bộ" }';
COMMENT ON COLUMN students.wallet_balance IS '{ "label": "Số dư ví", "type": "decimal", "description": "Số tiền học phí tích lũy trong ví nội bộ (không hoàn tiền mặt)", "visible": true }';

-- 5. Đăng ký lớp học
CREATE TABLE IF NOT EXISTS enrollments (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    class_id INTEGER REFERENCES classes(id),
    student_id INTEGER REFERENCES students(id),
    enrollment_date DATE DEFAULT CURRENT_DATE,
    status TEXT DEFAULT 'enrolled' CHECK (status IN ('enrolled', 'studying', 'completed', 'reserved', 'dropped')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

-- 6. Điểm danh & Nhận xét buổi học
CREATE TABLE IF NOT EXISTS attendance (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    class_id INTEGER REFERENCES classes(id),
    student_id INTEGER REFERENCES students(id),
    session_date DATE NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('present', 'absent_excused', 'absent_no_excuse')),
    teacher_comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE attendance IS '{ "label": "Điểm danh", "description": "Ghi nhận chuyên cần và nhận xét của giáo viên sau mỗi buổi dạy" }';

-- 7. Đánh giá từ phụ huynh (Feedback)
CREATE TABLE IF NOT EXISTS parent_feedbacks (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    attendance_id INTEGER REFERENCES attendance(id),
    rating_content INTEGER CHECK (rating_content BETWEEN 1 AND 5),
    rating_teacher INTEGER CHECK (rating_teacher BETWEEN 1 AND 5),
    rating_facility INTEGER CHECK (rating_facility BETWEEN 1 AND 5),
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE parent_feedbacks IS '{ "label": "Đánh giá buổi học", "description": "Phản hồi của phụ huynh về chất lượng buổi học qua App" }';

-- 8. Giao dịch tài chính & Ví nội bộ
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    student_id INTEGER REFERENCES students(id),
    amount NUMERIC(15, 2) NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('deposit', 'tuition_deduction', 'refund_to_wallet', 'adjustment')),
    description TEXT,
    misa_invoice_id TEXT, -- Liên kết mã hóa đơn MISA
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE transactions IS '{ "label": "Giao dịch", "description": "Lịch sử biến động số dư ví học viên và nộp phí" }';

-- 9. CRM: Chiến dịch Marketing
CREATE TABLE IF NOT EXISTS campaigns (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    title TEXT NOT NULL,
    budget NUMERIC(15, 2) DEFAULT 0,
    start_date DATE,
    end_date DATE,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'paused', 'completed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE campaigns IS '{ "label": "Chiến dịch", "description": "Quản lý các chiến dịch tuyển sinh Marketing" }';
COMMENT ON COLUMN campaigns.title IS '{ "label": "Tên chiến dịch", "type": "short_text", "required": true, "description": "Tên chiến dịch quảng cáo", "visible": true }';
COMMENT ON COLUMN campaigns.budget IS '{ "label": "Ngân sách", "type": "decimal", "required": true, "description": "Tổng ngân sách cho chiến dịch", "visible": true }';
COMMENT ON COLUMN campaigns.created_at IS '{ "label": "Ngày tạo", "type": "datetime", "required": false, "description": "Ngày tạo", "visible": true }';
COMMENT ON COLUMN campaigns.updated_at IS '{ "label": "Ngày cập nhật", "type": "datetime", "required": false, "description": "Ngày cập nhật", "visible": false }';
COMMENT ON COLUMN campaigns.deleted_at IS '{ "label": "Ngày xóa", "type": "datetime", "required": false, "description": "Ngày xóa", "visible": false }';

-- 9.1 CRM: Leads (Khách hàng tiềm năng)
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    campaign_id INTEGER REFERENCES campaigns(id),
    full_name TEXT NOT NULL,
    phone TEXT NOT NULL,
    email TEXT,
    source TEXT, -- Facebook, Website, Referral
    status TEXT DEFAULT 'new' CHECK (status IN ('new', 'contacted', 'trial', 'closed_won', 'closed_lost')),
    assigned_to INTEGER REFERENCES users(id), -- Giao cho Sale nào
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE leads IS '{ "label": "Khách hàng tiềm năng", "description": "Quản lý phễu khách hàng từ lúc phát sinh đến khi chuyển đổi" }';
COMMENT ON COLUMN leads.full_name IS '{ "label": "Họ tên", "type": "short_text", "required": true, "description": "Họ tên khách hàng", "visible": true }';
COMMENT ON COLUMN leads.phone IS '{ "label": "Số điện thoại", "type": "short_text", "required": true, "description": "Số điện thoại liên hệ", "visible": true }';
COMMENT ON COLUMN leads.status IS '{ "label": "Trạng thái", "type": "select", "required": true, "description": "Trạng thái: new, contacted, trial, closed_won, closed_lost", "enums": [{"label": "Mới", "value": "new"}, {"label": "Đang tư vấn", "value": "contacted"}] }';
COMMENT ON COLUMN leads.created_at IS '{ "label": "Ngày tạo", "type": "datetime", "required": false, "description": "Ngày tạo", "visible": true }';
COMMENT ON COLUMN leads.updated_at IS '{ "label": "Ngày cập nhật", "type": "datetime", "required": false, "description": "Ngày cập nhật", "visible": false }';
COMMENT ON COLUMN leads.deleted_at IS '{ "label": "Ngày xóa", "type": "datetime", "required": false, "description": "Ngày xóa", "visible": false }';

-- 9.2 CRM: Sale Tasks
CREATE TABLE IF NOT EXISTS sales_tasks (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    lead_id INTEGER REFERENCES leads(id),
    staff_id INTEGER REFERENCES users(id),
    task_type TEXT CHECK (task_type IN ('call', 'message', 'email', 'meeting')),
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'completed', 'overdue')),
    response_time_seconds INTEGER, -- Target 30s
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE sales_tasks IS '{ "label": "Công việc Sale", "description": "Theo dõi các tác vụ chăm sóc khách hàng của nhân viên tuyển sinh" }';
COMMENT ON COLUMN sales_tasks.response_time_seconds IS '{ "label": "Thời gian phản hồi", "type": "number", "required": false, "description": "Thời gian từ lúc lead phát sinh đến lúc xử lý (giây)", "visible": true }';

-- 10. Kho học liệu & Vật tư
CREATE TABLE IF NOT EXISTS inventory_items (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    sku TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    category TEXT CHECK (category IN ('book', 'uniform', 'equipment', 'other')),
    unit TEXT, -- Cuốn, Bộ, Cái
    stock_quantity INTEGER DEFAULT 0,
    min_stock_level INTEGER DEFAULT 5,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE inventory_items IS '{ "label": "Vật tư học liệu", "description": "Quản lý tồn kho sách, giáo trình và thiết bị" }';
COMMENT ON COLUMN inventory_items.sku IS '{ "label": "Mã SKU", "type": "short_text", "required": true, "description": "Mã quản lý kho duy nhất", "visible": true }';
COMMENT ON COLUMN inventory_items.name IS '{ "label": "Tên vật tư", "type": "short_text", "required": true, "description": "Tên vật tư học liệu", "visible": true }';
COMMENT ON COLUMN inventory_items.stock_quantity IS '{ "label": "Số lượng tồn", "type": "number", "required": true, "description": "Số lượng hiện có trong kho", "visible": true }';
COMMENT ON COLUMN inventory_items.created_at IS '{ "label": "Ngày tạo", "type": "datetime", "required": false, "description": "Ngày tạo", "visible": true }';
COMMENT ON COLUMN inventory_items.updated_at IS '{ "label": "Ngày cập nhật", "type": "datetime", "required": false, "description": "Ngày cập nhật", "visible": false }';
COMMENT ON COLUMN inventory_items.deleted_at IS '{ "label": "Ngày xóa", "type": "datetime", "required": false, "description": "Ngày xóa", "visible": false }';

-- 10.1 Lịch sử Xuất/Nhập kho
CREATE TABLE IF NOT EXISTS inventory_transactions (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    item_id INTEGER REFERENCES inventory_items(id),
    type TEXT NOT NULL CHECK (type IN ('in', 'out')),
    reason TEXT NOT NULL CHECK (reason IN ('purchase', 'adjustment', 'student_issue', 'teacher_issue', 'damage')),
    target_user_id INTEGER REFERENCES users(id), -- Người nhận (GV/HV)
    quantity INTEGER NOT NULL,
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE inventory_transactions IS '{ "label": "Giao dịch kho", "description": "Lịch sử nhập xuất vật tư học liệu" }';
COMMENT ON COLUMN inventory_transactions.type IS '{ "label": "Loại", "type": "select", "required": true, "description": "Nhập (in) hoặc Xuất (out)", "enums": [{"label": "Nhập", "value": "in"}, {"label": "Xuất", "value": "out"}] }';

-- 11. Học thuật: Bảng điểm & Kết quả
CREATE TABLE IF NOT EXISTS grades (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    student_id INTEGER REFERENCES students(id),
    enrollment_id INTEGER REFERENCES enrollments(id),
    title TEXT NOT NULL, -- "Kiểm tra định kỳ 1", "Giữa kỳ"
    score NUMERIC(4, 2),
    teacher_comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE grades IS '{ "label": "Bảng điểm", "description": "Lưu trữ kết quả các bài kiểm tra của học viên" }';
COMMENT ON COLUMN grades.title IS '{ "label": "Tên bài kiểm tra", "type": "short_text", "required": true, "description": "Tên kỳ kiểm tra", "visible": true }';
COMMENT ON COLUMN grades.score IS '{ "label": "Điểm số", "type": "decimal", "required": true, "description": "Điểm đạt được (0-10)", "visible": true }';
COMMENT ON COLUMN grades.created_at IS '{ "label": "Ngày tạo", "type": "datetime", "required": false, "description": "Ngày tạo", "visible": true }';
COMMENT ON COLUMN grades.updated_at IS '{ "label": "Ngày cập nhật", "type": "datetime", "required": false, "description": "Ngày cập nhật", "visible": false }';
COMMENT ON COLUMN grades.deleted_at IS '{ "label": "Ngày xóa", "type": "datetime", "required": false, "description": "Ngày xóa", "visible": false }';

-- 11.1 Học thuật: Bảo lưu kết quả
CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    student_id INTEGER REFERENCES students(id),
    class_id INTEGER REFERENCES classes(id),
    start_date DATE NOT NULL,
    expected_return_date DATE,
    reserved_amount NUMERIC(15, 2) DEFAULT 0,
    reason TEXT,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'returned', 'expired')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE reservations IS '{ "label": "Bảo lưu", "description": "Quản lý các yêu cầu bảo lưu học phí của học viên" }';
COMMENT ON COLUMN reservations.reserved_amount IS '{ "label": "Phí bảo lưu", "type": "decimal", "required": true, "description": "Số tiền được bảo lưu vào ví nội bộ", "visible": true }';

-- 12. Học liệu & Bài tập (Digital Library)
CREATE TABLE IF NOT EXISTS materials (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    title TEXT NOT NULL,
    category TEXT, -- Giáo án, Bài giảng, Đề thi
    type TEXT CHECK (type IN ('pdf', 'slide', 'doc', 'image', 'video')),
    file_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE materials IS '{ "label": "Học liệu số", "description": "Kho lưu trữ tài liệu giảng dạy cho giáo viên" }';
COMMENT ON COLUMN materials.title IS '{ "label": "Tiêu đề", "type": "short_text", "required": true, "description": "Tên tài liệu", "visible": true }';
COMMENT ON COLUMN materials.created_at IS '{ "label": "Ngày tạo", "type": "datetime", "required": false, "description": "Ngày tạo", "visible": true }';
COMMENT ON COLUMN materials.updated_at IS '{ "label": "Ngày cập nhật", "type": "datetime", "required": false, "description": "Ngày cập nhật", "visible": false }';
COMMENT ON COLUMN materials.deleted_at IS '{ "label": "Ngày xóa", "type": "datetime", "required": false, "description": "Ngày xóa", "visible": false }';

CREATE TABLE IF NOT EXISTS assignments (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    class_id INTEGER REFERENCES classes(id),
    teacher_id INTEGER REFERENCES users(id),
    material_id INTEGER REFERENCES materials(id),
    title TEXT NOT NULL,
    instructions TEXT,
    deadline TIMESTAMP,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'closed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE assignments IS '{ "label": "Bài tập về nhà", "description": "Danh sách bài tập giáo viên giao cho lớp" }';
COMMENT ON COLUMN assignments.deadline IS '{ "label": "Hạn nộp", "type": "datetime", "required": true, "description": "Thời hạn cuối cùng để học viên nộp bài", "visible": true }';
COMMENT ON COLUMN assignments.created_at IS '{ "label": "Ngày tạo", "type": "datetime", "required": false, "description": "Ngày tạo", "visible": true }';
COMMENT ON COLUMN assignments.updated_at IS '{ "label": "Ngày cập nhật", "type": "datetime", "required": false, "description": "Ngày cập nhật", "visible": false }';
COMMENT ON COLUMN assignments.deleted_at IS '{ "label": "Ngày xóa", "type": "datetime", "required": false, "description": "Ngày xóa", "visible": false }';

CREATE TABLE IF NOT EXISTS student_submissions (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    assignment_id INTEGER REFERENCES assignments(id),
    student_id INTEGER REFERENCES students(id),
    submission_url TEXT,
    score NUMERIC(4, 2),
    feedback TEXT,
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE student_submissions IS '{ "label": "Bài nộp học viên", "description": "Kết quả nộp bài và chấm điểm từ giáo viên" }';

-- 13. Mạng xã hội & Tin nhắn (Community)
CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    author_id INTEGER REFERENCES users(id),
    content TEXT NOT NULL,
    image_url TEXT,
    post_type TEXT DEFAULT 'general' CHECK (post_type IN ('general', 'announcement', 'moment')),
    likes_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE posts IS '{ "label": "Bài viết", "description": "Bảng tin cộng đồng phụ huynh và thông báo trung tâm" }';

CREATE TABLE IF NOT EXISTS post_comments (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    post_id INTEGER REFERENCES posts(id),
    author_id INTEGER REFERENCES users(id),
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE post_comments IS '{ "label": "Bình luận", "description": "Tương tác trên các bài viết cộng đồng" }';

CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE conversations IS '{ "label": "Cuộc hội thoại", "description": "Quản lý các luồng chat giữa người dùng" }';

CREATE TABLE IF NOT EXISTS conversation_participants (
    conversation_id INTEGER REFERENCES conversations(id),
    user_id INTEGER REFERENCES users(id),
    PRIMARY KEY (conversation_id, user_id)
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    conversation_id INTEGER REFERENCES conversations(id),
    sender_id INTEGER REFERENCES users(id),
    content TEXT,
    image_url TEXT,
    is_read BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

COMMENT ON TABLE chat_messages IS '{ "label": "Tin nhắn", "description": "Chi tiết tin nhắn trong cuộc hội thoại" }';

-- SEED DATA
-- Tạo Quyền hạn cơ bản
INSERT INTO permissions (module, key, label) VALUES 
('CRM', 'crm.leads.view', 'Xem danh sách Lead'),
('Tài chính', 'finance.revenue.view', 'Xem báo cáo doanh thu'),
('Học vụ', 'academic.classes.manage', 'Quản lý lớp học');

-- Tạo Vai trò mẫu
INSERT INTO roles (name, description) VALUES 
('Kế toán trưởng', 'Quản lý dòng tiền và hóa đơn MISA'),
('Nhân viên Tuyển sinh', 'Xử lý Lead và Sale Task');

-- Tạo Người dùng
INSERT INTO users (username, full_name, email, password, role, status, base_salary)
VALUES ('admin', 'Nguyễn Quản Trị', 'admin@zenmaster.edu.vn', 'hashed_password_123', 'admin', 'active', 15000000);

INSERT INTO users (username, full_name, email, password, zalo_id, role, status, teaching_rate)
VALUES ('teacher_minh', 'Mr. Johnathan', 'minh.teacher@gmail.com', 'hashed_password_456', 'zalo_minh_123', 'teacher', 'active', 500000);

INSERT INTO users (username, full_name, email, password, zalo_id, role, status)
VALUES ('parent_lan', 'Trần Thị Bưởi', 'lan.parent@gmail.com', 'hashed_password_789', 'zalo_lan_456', 'parent', 'active');

-- Tạo Chiến dịch & Lead
INSERT INTO campaigns (title, budget, status) VALUES ('Tuyển sinh Khóa Hè 2024', 15000000, 'active');
INSERT INTO leads (campaign_id, full_name, phone, source, status, assigned_to)
VALUES (1, 'Nguyễn Minh Anh', '0901234567', 'Facebook Ads', 'contacted', 1);

-- Tạo Khóa học & Lớp học
INSERT INTO courses (title, description, base_price, duration_weeks)
VALUES ('Zen Foundation L1', 'Khóa học tiếng Anh nền tảng cho người mới bắt đầu', 5000000, 12);

INSERT INTO classes (course_id, teacher_id, name, room, start_date, status)
VALUES (1, 2, 'Lớp ZEN-F01', 'Phòng 302', '2024-01-15', 'active');

-- Tạo Học viên & Ví
INSERT INTO students (user_id, parent_id, full_name, student_code, wallet_balance)
VALUES (NULL, 3, 'Nguyễn Minh Anh', 'HV-2024-001', 2500000);

-- Tạo Vật tư & Giao dịch kho
INSERT INTO inventory_items (sku, name, category, unit, stock_quantity)
VALUES ('BK-ZEN-F1', 'Giáo trình Zen Foundation L1', 'book', 'Cuốn', 50);

INSERT INTO inventory_transactions (item_id, type, reason, target_user_id, quantity, note)
VALUES (1, 'out', 'student_issue', 3, 1, 'Cấp phát cho học viên mới');

-- Tạo Bảng điểm mẫu
INSERT INTO grades (student_id, title, score, teacher_comment)
VALUES (1, 'Kiểm tra định kỳ 1', 9.0, 'Nắm vững kiến thức, phát âm tốt');

-- Tạo Bài viết cộng đồng
INSERT INTO posts (author_id, content, post_type, likes_count)
VALUES (2, 'Các con lớp Panda 01 đã có một buổi học về màu sắc rất sôi nổi!', 'moment', 12);
`

// 1. DDL & Data Mutation
await db.exec(SQL);



const allUsers = await db.query(`SELECT * FROM "users"`);
console.table(allUsers);
