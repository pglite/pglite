fn main() {
    #[cfg(all(feature = "napi-binding", not(target_arch = "wasm32")))]
    {
        napi_build::setup();
    }
}

