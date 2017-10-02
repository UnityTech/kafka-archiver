# kafka-archiver.yaml

# Secrets
The deployment makes use of Kubernetes Secrets to store S3 credentials.
To create a new secret, follow the example below. Note that the secret has to
be in the same namespace where you deployed kafka-archiver (`default` in this example).

```bash
kubectl create secret --namespace default generic kafka-archiver --from-literal=AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXXX --from-literal=AWS_SECRET_ACCESS_KEY=@yuBXGUErJVPntm[7ty99y9C
```