//! Kênh báo lỗi cho bộ đánh giá biểu thức (`eval_sql_expr`, `CompiledExpr::eval`): các hàm này trả
//! `Value`, không trả `Result`, nên lỗi lúc chạy (chia cho 0, ép kiểu sai, tràn số...) được ghi vào
//! đây rồi trả NULL. `Executor::execute` lấy lỗi ra sau mỗi câu lệnh và trả `Err` — statement guard
//! hoàn tác câu đó như mọi lỗi khác. Chỉ giữ lỗi ĐẦU TIÊN.
use std::cell::RefCell;

thread_local! {
    static PENDING: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Ghi lỗi (nếu chưa có lỗi nào đang chờ).
pub(crate) fn raise(msg: impl Into<String>) {
    PENDING.with(|p| {
        let mut p = p.borrow_mut();
        if p.is_none() {
            *p = Some(msg.into());
        }
    });
}

pub(crate) fn take() -> Option<String> {
    PENDING.with(|p| p.borrow_mut().take())
}

pub(crate) fn clear() {
    PENDING.with(|p| *p.borrow_mut() = None);
}

pub(crate) fn division_by_zero() {
    raise("division by zero");
}
