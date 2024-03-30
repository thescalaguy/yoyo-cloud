# ☁️ Yoyo Cloud ☁️
## Run SQL migrations from cloud storage  

Yoyo cloud builds on top of [Yoyo migrations](https://ollycope.com/software/yoyo/latest/) and allows appying SQL migrations that are stored in cloud storage.

## Example  
### Applying from S3
```python
from yoyo import get_backend
from yoyo_cloud import read_s3_migrations

if __name__ == "__main__":
    migrations = read_s3_migrations(paths=["s3://bucket/yoyo-migrations-s3/"])
    backend = get_backend(f"postgresql://postgres:my-secret-pw@localhost:5432/postgres")

    with backend.lock():
        # -- Apply any outstanding migrations
        backend.apply_migrations(backend.to_apply(migrations))
```