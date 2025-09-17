import os
import re
import json  # 添加json模块


def clean_formula_line(line: str) -> str:
    """
    清理公式行：去掉 %F 和序列编号 + 人名和日期
    """
    # 删除 %F 和序列编号
    line = re.sub(r"^%F\s+[A-Za-z0-9]+", "", line).strip()

    # 删除人名和日期部分
    line = re.sub(r" - _.*$", "", line)

    return line.strip()


def remove_from_start_end_content(lines: list) -> list:
    """
    删除 From 到 (Start) 和 (End) 之间的内容（包括 From 和 (End) 行）
    """
    result_lines = []
    skip = False

    for line in lines:
        # 如果遇到 From 和 (Start)，则开始跳过接下来的行，直到 (End)
        if "From" in line and "(Start)" in line:
            skip = True

        if not skip:
            result_lines.append(line)  # 添加不需要跳过的行

        # 如果遇到 (End)，则停止跳过并继续处理后续的行
        if skip and "(End)" in line:
            skip = False

    return result_lines


def remove_conjecture_lines(lines: list) -> list:
    """
    删除包含 "Conjecture" 的行
    """
    return [line for line in lines if "Conjecture" not in line]


def extract_F_lines(src_root, dst_root):
    if not os.path.exists(dst_root):
        os.makedirs(dst_root)

    single_line_count = 0  # 统计只剩一行公式的序列数
    total_sequences = 0  # 统计总共处理的序列数

    for folder in sorted(os.listdir(src_root)):
        folder_path = os.path.join(src_root, folder)
        if not os.path.isdir(folder_path):
            continue

        dst_folder = os.path.join(dst_root, folder.lower())  # a000 格式
        os.makedirs(dst_folder, exist_ok=True)

        for file in sorted(os.listdir(folder_path)):
            if not file.endswith(".seq"):
                continue

            file_path = os.path.join(folder_path, file)
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()

            # Step 1: 删除 From 和 (Start) 到 (End) 之间的内容
            lines = remove_from_start_end_content(lines)

            # Step 2: 删除包含 "Conjecture" 的行
            lines = remove_conjecture_lines(lines)

            formulas = []
            for line in lines:
                if line.startswith("%F"):
                    # Step 3: 清理公式
                    text = clean_formula_line(line)
                    if text:  # 避免空行
                        formulas.append(text)

            if formulas:  # 只在有内容时生成文件
                # 获取序列ID（从文件名）
                sequence_id = file.replace(".seq", "")

                # 创建JSON数据结构
                json_data = {
                    "sequence_id": sequence_id,
                    "formulas": formulas,
                    "formula_count": len(formulas)
                }

                # 保存为JSON文件
                dst_file = os.path.join(dst_folder, file.replace(".seq", ".json"))
                with open(dst_file, "w", encoding="utf-8") as out:
                    json.dump(json_data, out, indent=2, ensure_ascii=False)

                # 统计只剩一行公式的序列
                if len(formulas) == 1:
                    single_line_count += 1

                # 统计总处理序列数
                total_sequences += 1

        print(f"📂 处理完成文件夹 {folder}")

    print(f"✅ %F 行提取并清理完成！")
    print(f"📊 总共有 {single_line_count} 个序列只剩下了一行公式。")
    print(f"📊 总共有 {total_sequences} 个序列被处理！")


if __name__ == "__main__":
    src_root = r"oeis"  # 原始 OEIS 数据路径
    dst_root = r"oeis_onlyclean_json"  # 输出路径（改为json）
    extract_F_lines(src_root, dst_root)