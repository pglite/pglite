
import { Glob } from "bun";

async function main() {
  console.log("Bạn muốn làm prompt nào?\n");
  console.log("1. Kiểm tra xem còn thiết tính năng nào? (Để tự động xây dựng engine các tính năng còn thiếu)");

  // Hàm prompt được tích hợp sẵn dưới dạng Global API trong Bun
  const choice = prompt("\nNhập lựa chọn của bạn (mặc định là 1): ");
  let promptText = "";

  if (choice === "1" || !choice) {
    promptText = "Kiểm tra xem còn thiếu tính năng nào? (Để tự động xây dựng engine các tính năng còn thiếu)\n\nDưới đây là toàn bộ source code của dự án:";
  } else {
    promptText = "Kiểm tra xem còn thiếu tính năng nào? (Để tự động xây dựng engine các tính năng còn thiếu)\n\nDưới đây là toàn bộ source code của dự án:";
  }

  // Quét toàn bộ file trong thư mục
  const glob = new Glob("**/*");
  const files = Array.from(glob.scanSync("."))
    .filter(file => {
      // Loại bỏ các thư mục, file cấu hình build, binary, và file prompt được tạo ra
      if (
        file.startsWith("node_modules/") ||
        file.startsWith(".git/") ||
        file.startsWith(".idea/") ||
        file.includes("dist/") ||
        file.includes("out/") ||
        file.includes("coverage/") ||
        file.endsWith(".DS_Store") ||
        file.endsWith(".tgz") ||
        file.endsWith("bun.lockb") ||
        file.endsWith("bun.lock") ||
        file.endsWith(".sql") ||
        file.endsWith(".wal") ||
        file.endsWith(".db") ||
        file === "generated_prompt.md"
      ) {
        return false;
      }
      if (file.includes("pglite-rust")) {
        return false;
      }
      if (!file.includes("src/")) {
        return false;
      }
      return true;
    });

  let outputText = promptText + "\n\n";

  for (const path of files) {
    try {
      const file = Bun.file(path);
      const content = await file.text();
      
      // Lấy đuôi file để syntax highlight phù hợp (vd: ts, json, md), nếu không có trả về txt
      const parts = path.split(".");
      let ext = parts.length > 1 ? parts.pop() : "txt";
      if (ext === "gitignore") ext = "gitignore"; // Đặc biệt cho file .gitignore
      
      outputText += `### File: ${path}\n`;
      outputText += `\`\`\`${ext}\n`;
      outputText += content;
      if (!content.endsWith("\n")) {
        outputText += "\n";
      }
      outputText += `\`\`\`\n\n`;
    } catch (e) {
      console.error(`Không thể đọc file: ${path}`);
    }
  }

  // Lưu ra một file kết quả để dễ copy
  const outPath = "generated_prompt.md";
  await Bun.write(outPath, outputText);
  
  console.log(`\n✅ Đã gom code và lưu vào file '${outPath}' thành công!`);
  console.log("👉 Bạn chỉ cần mở file 'generated_prompt.md' lên, copy tất cả (Ctrl/Cmd + A) và dán cho AI.");
}

main();