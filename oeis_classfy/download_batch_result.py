import os
import json
import time
from zhipuai import ZhipuAI

# 初始化智谱AI客户端 - 请使用与提交时相同的API密钥
client = ZhipuAI(api_key="api_key")


def check_and_download_results(task_id_file, output_base_dir="batch_results"):
    """
    检查多个任务状态并下载所有结果
    """
    # 读取所有任务ID
    if not os.path.exists(task_id_file):
        print(f"❌ 任务ID文件不存在: {task_id_file}")
        return

    with open(task_id_file, 'r') as f:
        task_ids = [line.strip() for line in f if line.strip()]

    if not task_ids:
        print("❌ 任务ID文件中没有有效的任务ID")
        return

    print(f"📋 找到 {len(task_ids)} 个任务ID")

    # 为每个任务创建单独的输出目录
    for i, task_id in enumerate(task_ids, 1):
        task_output_dir = os.path.join(output_base_dir, f"task_{i}")

        # 确保输出目录存在
        os.makedirs(task_output_dir, exist_ok=True)

        output_result_file = os.path.join(task_output_dir, "batch_output.jsonl")

        print(f"\n🔍 处理任务 {i}/{len(task_ids)}: {task_id}")
        check_and_download_result(task_id, output_result_file, task_output_dir)


def check_and_download_result(batch_id, output_result_path, output_dir):
    """
    检查单个任务状态并下载结果
    """
    print(f"  ⏳ 检查任务状态...")

    try:
        batch_status = client.batches.retrieve(batch_id)
        status = batch_status.status
        print(f"  📊 任务状态: {status}")

        if status == "completed":
            print("  🎉 任务已完成，开始下载结果...")

            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_result_path), exist_ok=True)

            # 下载结果文件
            if batch_status.output_file_id:
                content = client.files.content(batch_status.output_file_id)
                content.write_to_file(output_result_path)
                print(f"  ✅ 结果已下载至: {output_result_path}")

                # 处理结果
                process_results(output_result_path, output_dir)
            else:
                print("  ⚠️  无输出文件ID")

            # 下载错误信息（如果有）
            if batch_status.error_file_id:
                error_content = client.files.content(batch_status.error_file_id)
                error_file_path = os.path.join(output_dir, "batch_errors.jsonl")
                error_content.write_to_file(error_file_path)
                print(f"  ⚠️  错误信息已下载至: {error_file_path}")

            return True

        elif status in ["validating", "in_progress", "finalizing"]:
            print(f"  ⏳ 任务仍在处理中 ({status})")
            print("  💡 请稍后再运行此脚本")
            return False

        elif status in ["failed", "expired", "cancelled"]:
            print(f"  ❌ 任务异常终止: {status}")

            # 即使任务失败，也尝试下载错误信息
            if batch_status.error_file_id:
                error_content = client.files.content(batch_status.error_file_id)
                error_file_path = os.path.join(output_dir, "batch_errors.jsonl")
                error_content.write_to_file(error_file_path)
                print(f"  ⚠️  错误信息已下载至: {error_file_path}")

            return True

    except Exception as e:
        print(f"  ❌ 检查任务状态时出错: {e}")
        return False

    return True


def process_results(result_file_path, output_dir):
    """
    处理结果文件 - 专门处理公式分类结果
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    formula_type_counts = {}
    total_formulas = 0
    processed_sequences = 0
    failed_sequences = 0

    try:
        with open(result_file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    # 解析响应行
                    response_data = json.loads(line.strip())

                    # 提取响应状态和主体
                    response_status = response_data.get('status_code', 200)
                    if response_status != 200:
                        print(f"  ⚠️ 第 {line_num} 行请求失败，状态码: {response_status}")
                        failed_sequences += 1
                        continue

                    # 提取模型响应
                    response_body = response_data.get('response', {}).get('body', {})
                    choices = response_body.get('choices', [])

                    if choices:
                        message_content = choices[0].get('message', {}).get('content', '{}')

                        # 解析模型返回的JSON内容
                        try:
                            result = json.loads(message_content)
                        except json.JSONDecodeError:
                            print(f"  ❌ 第 {line_num} 行: 无法解析模型返回的JSON内容")
                            failed_sequences += 1
                            continue

                        # 提取序列ID
                        sequence_id = result.get('sequence_id', f'unknown_{line_num}')

                        # 保存单个序列的结果
                        output_file = os.path.join(output_dir, f"{sequence_id}_classified.json")
                        with open(output_file, 'w', encoding='utf-8') as out_f:
                            json.dump(result, out_f, indent=2, ensure_ascii=False)

                        # 统计公式类型
                        extracted_formulas = result.get('extracted_formulas', [])
                        for formula in extracted_formulas:
                            formula_type = formula.get('formula_type', 'unknown')
                            formula_type_counts[formula_type] = formula_type_counts.get(formula_type, 0) + 1
                            total_formulas += 1

                        processed_sequences += 1

                        # 每处理100个序列打印一次进度
                        if processed_sequences % 100 == 0:
                            print(f"  📊 已处理 {processed_sequences} 个序列，{total_formulas} 个公式")

                except Exception as e:
                    print(f"  ❌ 处理第 {line_num} 行时出错: {e}")
                    failed_sequences += 1
                    continue

    except FileNotFoundError:
        print(f"  ❌ 结果文件不存在: {result_file_path}")
        return

    # 保存统计信息
    stats_file = os.path.join(output_dir, "formula_type_statistics.json")
    with open(stats_file, 'w', encoding='utf-8') as f:
        stats_data = {
            "total_sequences": processed_sequences + failed_sequences,
            "successful_sequences": processed_sequences,
            "failed_sequences": failed_sequences,
            "total_formulas": total_formulas,
            "type_counts": formula_type_counts
        }

        # 计算百分比（避免除以零）
        if total_formulas > 0:
            stats_data["type_percentages"] = {
                k: round(v / total_formulas * 100, 2) for k, v in formula_type_counts.items()
            }

        json.dump(stats_data, f, indent=2, ensure_ascii=False)

    print(f"  📊 处理完成! 成功处理 {processed_sequences} 个序列，失败 {failed_sequences} 个序列")
    print(f"  📊 总共提取 {total_formulas} 个公式")
    print(f"  📈 统计信息已保存至: {stats_file}")

    # 打印简要统计
    if total_formulas > 0:
        print(f"\n  📊 公式类型分布:")
        for formula_type, count in sorted(formula_type_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = round(count / total_formulas * 100, 2)
            print(f"    {formula_type}: {count} ({percentage}%)")


def check_batch_status_only(task_id_file):
    """
    仅检查任务状态，不下载结果
    """
    # 读取所有任务ID
    if not os.path.exists(task_id_file):
        print(f"❌ 任务ID文件不存在: {task_id_file}")
        return

    with open(task_id_file, 'r') as f:
        task_ids = [line.strip() for line in f if line.strip()]

    if not task_ids:
        print("❌ 任务ID文件中没有有效的任务ID")
        return

    print(f"📋 找到 {len(task_ids)} 个任务ID")

    completed = 0
    in_progress = 0
    failed = 0

    for i, task_id in enumerate(task_ids, 1):
        print(f"\n🔍 检查任务 {i}/{len(task_ids)}: {task_id}")

        try:
            batch_status = client.batches.retrieve(task_id)
            status = batch_status.status
            print(f"  📊 任务状态: {status}")

            if status == "completed":
                completed += 1
            elif status in ["validating", "in_progress", "finalizing"]:
                in_progress += 1
            else:  # failed, expired, cancelled
                failed += 1

        except Exception as e:
            print(f"  ❌ 检查任务状态时出错: {e}")
            failed += 1

    print(f"\n📊 任务状态汇总:")
    print(f"  ✅ 已完成: {completed}")
    print(f"  ⏳ 处理中: {in_progress}")
    print(f"  ❌ 失败: {failed}")

    if in_progress == 0 and completed > 0:
        print("\n🎉 所有任务已完成，可以运行下载脚本获取结果!")
    elif in_progress > 0:
        print(f"\n⏳ 仍有 {in_progress} 个任务在处理中，请稍后再检查")


if __name__ == "__main__":
    task_id_file = "batch_task_ids.txt"  # 保存所有任务ID的文件
    output_base_dir = "batch_results"  # 结果文件的基础目录

    print("🔍 智谱AI Batch任务结果下载工具")
    print("=" * 50)

    # 提供选项：仅检查状态或下载结果
    print("\n请选择操作:")
    print("1. 仅检查任务状态")
    print("2. 检查并下载结果")

    choice = input("请输入选项 (1 或 2): ").strip()

    if choice == "1":
        print("\n" + "=" * 50)
        print("仅检查任务状态")
        print("=" * 50)
        check_batch_status_only(task_id_file)
    elif choice == "2":
        print("\n" + "=" * 50)
        print("检查任务状态并下载结果")
        print("=" * 50)
        check_and_download_results(task_id_file, output_base_dir)
    else:
        print("❌ 无效选项，请输入 1 或 2")