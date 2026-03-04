from datetime import datetime, timedelta

# Generate date ranges to support batch daily proccessing
def generate_date_ranges(start_date, end_date, days_interval):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_ranges = []
    current = start
    while current <= end:
        batch_end = current + timedelta(days=days_interval - 1)
        if batch_end > end:
            batch_end = end
        date_ranges.append((current.strftime("%Y-%m-%d"), batch_end.strftime("%Y-%m-%d")))
        current = batch_end + timedelta(days=1)
    return date_ranges


def print_etl_summary(date_ranges, successful_batches, failed_batches, total_records, total_processing_time):
    print("\n" + "="*60)
    print("TRANSFORMATION SUMMARY")
    print("="*60)
    print(f"Total batches: {len(date_ranges)}")
    print(f"Successful batches: {successful_batches}")
    print(f"Failed batches: {len(failed_batches)}")
    print(f"Total records processed: {total_records:,}")
    print(f"Total processing time: {total_processing_time:.2f} seconds")
    print(f"Average time per batch: {total_processing_time/len(date_ranges):.2f} seconds")

    if failed_batches:
        print("\nFailed batches:")
        for batch_start, batch_end, error in failed_batches:
            print(f"  - {batch_start} to {batch_end}: {error}")
        import sys
        sys.exit(1)
    else:
        print("\nAll batches completed successfully!")