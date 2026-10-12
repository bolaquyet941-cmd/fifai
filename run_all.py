# run_all.py — Chạy br.py xong rồi chạy lw.py
import subprocess
import sys
import time

def run_script(name):
    print(f"\n{'='*60}")
    print(f"BẮT ĐẦU: {name}")
    print(f"{'='*60}\n")
    
    start = time.time()
    try:
        # Chạy script con, kế thừa stdin/stdout
        result = subprocess.run(
            [sys.executable, name],
            check=False,  # không raise nếu exit code != 0
        )
        elapsed = time.time() - start
        print(f"\n{name} kết thúc sau {elapsed/60:.1f} phút (exit={result.returncode})")
        return result.returncode == 0
    except KeyboardInterrupt:
        print(f"\n{name} bị dừng bởi người dùng")
        return False
    except Exception as e:
        print(f"\n{name} lỗi: {e}")
        return False

def main():
    print("="*60)
    print("  RUN ALL — br.py → lw.py")
    print("="*60)
    
    # Chạy br.py
    ok_br = run_script("runbr.py")
    if not ok_br: return
    # Dù br.py thành công hay không, vẫn chạy lw.py
    # (nếu muốn dừng khi br.py fail, sửa thành: if not ok_br: return)
    print("\n" + "="*60)
    print("  br.py xong, chuẩn bị chạy lw.py")
    print("="*60)
    time.sleep(5)  # chờ 5 giây trước khi chạy lw.py
    
    # Chạy lw.py
    ok_lw = run_script("runlw.py")
    
    print("\n" + "="*60)
    print("  HOÀN TẤT")
    print(f"  br.py: {'OK' if ok_br else 'FAIL'}")
    print(f"  lw.py: {'OK' if ok_lw else 'FAIL'}")
    print("="*60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[STOP]")
