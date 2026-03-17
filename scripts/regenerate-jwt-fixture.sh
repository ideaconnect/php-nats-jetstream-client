#!/usr/bin/env bash
# Rebuilds JWT/NKey fixture artifacts used by JWT integration tests.
#
# Usage:
#   bash scripts/regenerate-jwt-fixture.sh

set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
stage_dir="$tmp_dir/jwt"
nsc_dir="$tmp_dir/nsc"
output_dir="$root_dir/build/nats/jwt"
config_file="$root_dir/build/nats/jwt.conf"
nats_box_image="${NATS_BOX_IMAGE:-natsio/nats-box:latest}"

cleanup() {
  rm -rf "$tmp_dir"
}

trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

derive_public_key() {
  php -r 'require $argv[1]; $seed=trim((string) file_get_contents($argv[2])); $signer=new IDCT\NATS\Auth\NkeySeedSigner($seed); echo $signer->publicKey();' "$root_dir/vendor/autoload.php" "$1"
}

select_seed_for_public_key() {
  local expected_public_key="$1"
  local destination="$2"
  local candidate public_key

  for candidate in "$stage_dir"/raw-seeds/*.nk; do
    [[ -f "$candidate" ]] || continue
    public_key="$(derive_public_key "$candidate" 2>/dev/null || true)"
    if [[ "$public_key" == "$expected_public_key" ]]; then
      cp "$candidate" "$destination"
      return 0
    fi
  done

  echo "Unable to locate seed for public key: $expected_public_key" >&2
  exit 1
}

require_cmd docker
require_cmd php

if [[ ! -f "$root_dir/vendor/autoload.php" ]]; then
  echo "Missing vendor/autoload.php. Run composer install first." >&2
  exit 1
fi

mkdir -p "$stage_dir/raw-seeds"
mkdir -p "$nsc_dir"

docker run --rm -i \
  --user "$(id -u):$(id -g)" \
  -v "$stage_dir:/out" \
  -v "$nsc_dir:/nsc" \
  "$nats_box_image" sh <<'EOF'
set -e
export NSC_HOME=/nsc
mkdir -p "$NSC_HOME"
nsc add operator --name LOCALOP --sys >/dev/null
nsc add account --name LOCALACC >/dev/null
nsc add user --account LOCALACC --name localjwt >/dev/null
cp /nsc/nats/nsc/stores/LOCALOP/LOCALOP.jwt /out/operator.jwt
cp /nsc/nats/nsc/stores/LOCALOP/accounts/LOCALACC/LOCALACC.jwt /out/account.jwt
cp /nsc/nats/nsc/stores/LOCALOP/accounts/SYS/SYS.jwt /out/system-account.jwt
cp /nsc/nats/nsc/stores/LOCALOP/accounts/LOCALACC/users/localjwt.jwt /out/user.jwt
cp /nsc/nkeys/creds/LOCALOP/LOCALACC/localjwt.creds /out/user.creds
nsc describe operator --field sub | tr -d '"' > /out/operator.pub
nsc describe account --name LOCALACC --field sub | tr -d '"' > /out/account.pub
nsc describe account --name SYS --field sub | tr -d '"' > /out/system-account.pub
nsc describe user --account LOCALACC --name localjwt --field sub | tr -d '"' > /out/user.pub
find /nsc/nkeys/keys -name "*.nk" -exec cp {} /out/raw-seeds/ \;
EOF

awk '/BEGIN USER NKEY SEED/{getline; print; exit}' "$stage_dir/user.creds" > "$stage_dir/user.seed"

select_seed_for_public_key "$(tr -d '\r\n' < "$stage_dir/operator.pub")" "$stage_dir/operator.seed"
select_seed_for_public_key "$(tr -d '\r\n' < "$stage_dir/account.pub")" "$stage_dir/account.seed"

rm -rf "$output_dir"
mkdir -p "$output_dir"
cp "$stage_dir"/account.jwt "$output_dir/account.jwt"
cp "$stage_dir"/account.pub "$output_dir/account.pub"
cp "$stage_dir"/account.seed "$output_dir/account.seed"
cp "$stage_dir"/operator.jwt "$output_dir/operator.jwt"
cp "$stage_dir"/operator.pub "$output_dir/operator.pub"
cp "$stage_dir"/operator.seed "$output_dir/operator.seed"
cp "$stage_dir"/system-account.jwt "$output_dir/system-account.jwt"
cp "$stage_dir"/system-account.pub "$output_dir/system-account.pub"
cp "$stage_dir"/user.creds "$output_dir/user.creds"
cp "$stage_dir"/user.jwt "$output_dir/user.jwt"
cp "$stage_dir"/user.pub "$output_dir/user.pub"
cp "$stage_dir"/user.seed "$output_dir/user.seed"

cat > "$config_file" <<EOF
port: 4222
http_port: 8222
jetstream: true

operator: "/etc/nats-jwt/operator.jwt"
system_account: $(tr -d '\r\n' < "$output_dir/system-account.pub")
resolver: MEMORY

resolver_preload: {
  $(tr -d '\r\n' < "$output_dir/system-account.pub"): "$(tr -d '\r\n' < "$output_dir/system-account.jwt")"
  $(tr -d '\r\n' < "$output_dir/account.pub"): "$(tr -d '\r\n' < "$output_dir/account.jwt")"
}
EOF

if docker compose ps -q nats-jwt >/dev/null 2>&1; then
  container_id="$(docker compose ps -q nats-jwt | tr -d '\r\n')"
  if [[ -n "$container_id" ]]; then
    docker compose up -d --force-recreate nats-jwt >/dev/null
    echo "Recreated running nats-jwt service to pick up regenerated fixture artifacts."
  fi
fi

echo "Regenerated JWT fixture under $output_dir and $config_file"