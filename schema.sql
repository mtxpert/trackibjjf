-- =============================================================================
-- mattrack — Supabase schema
-- Run this in the Supabase SQL editor (Dashboard → SQL Editor → New query).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Extensions
-- ---------------------------------------------------------------------------
create extension if not exists "pgcrypto";   -- gen_random_uuid()


-- ---------------------------------------------------------------------------
-- public.users
-- ---------------------------------------------------------------------------
create table if not exists public.users (
    id                uuid        primary key references auth.users (id) on delete cascade,
    email             text        not null,
    plan              text        not null default 'free'
                                  check (plan in ('free', 'individual', 'gym', 'affiliate')),
    stripe_customer_id text,
    stripe_sub_id      text,
    sub_status         text        check (sub_status in ('active', 'canceled', 'past_due', 'trialing', null)),
    sub_expires_at     timestamptz,
    created_at         timestamptz not null default now()
);

comment on table public.users is
    'Extended profile for every authenticated user. Mirrors auth.users via trigger.';


-- ---------------------------------------------------------------------------
-- public.gym_packs
-- ---------------------------------------------------------------------------
create table if not exists public.gym_packs (
    id            uuid        primary key default gen_random_uuid(),
    owner_id      uuid        not null references public.users (id) on delete cascade,
    plan          text        not null default 'gym'
                              check (plan in ('gym', 'affiliate')),
    max_codes     integer     not null default 10 check (max_codes > 0),
    stripe_sub_id  text,
    sub_status     text        check (sub_status in ('active', 'canceled', 'past_due', 'trialing', null)),
    school_name   text        not null,
    school_slug   text        not null unique,
    created_at    timestamptz not null default now()
);

comment on table public.gym_packs is
    'Gym / affiliate subscription packs that generate access codes for members.';


-- ---------------------------------------------------------------------------
-- public.access_codes
-- ---------------------------------------------------------------------------
create table if not exists public.access_codes (
    id           uuid        primary key default gen_random_uuid(),
    code         text        not null unique,
    pack_id      uuid        not null references public.gym_packs (id) on delete cascade,
    redeemed_by  uuid        references public.users (id) on delete set null,
    redeemed_at  timestamptz,
    created_at   timestamptz not null default now()
);

comment on table public.access_codes is
    'Single-use access codes distributed by gym / affiliate pack owners.';

create index if not exists access_codes_pack_id_idx
    on public.access_codes (pack_id);

create index if not exists access_codes_redeemed_by_idx
    on public.access_codes (redeemed_by);


-- ---------------------------------------------------------------------------
-- Trigger: auto-create public.users row on auth sign-up
-- ---------------------------------------------------------------------------
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    insert into public.users (id, email)
    values (
        new.id,
        coalesce(new.email, '')
    )
    on conflict (id) do nothing;
    return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
    after insert on auth.users
    for each row execute procedure public.handle_new_user();


-- ---------------------------------------------------------------------------
-- Row-Level Security
-- ---------------------------------------------------------------------------

alter table public.users        enable row level security;
alter table public.gym_packs    enable row level security;
alter table public.access_codes enable row level security;

-- users: each user can read and update only their own row.
create policy "users: select own row"
    on public.users for select
    using (auth.uid() = id);

create policy "users: update own row"
    on public.users for update
    using (auth.uid() = id)
    with check (auth.uid() = id);

-- Service-role bypass is automatic in Supabase (service key ignores RLS).

-- gym_packs: owner can manage their own packs; members can read if sub active.
create policy "gym_packs: owner full access"
    on public.gym_packs for all
    using (auth.uid() = owner_id)
    with check (auth.uid() = owner_id);

create policy "gym_packs: members can read active packs"
    on public.gym_packs for select
    using (sub_status = 'active');

-- access_codes: pack owner can read/insert; redeemer can read their own code.
create policy "access_codes: pack owner can read"
    on public.access_codes for select
    using (
        pack_id in (
            select id from public.gym_packs where owner_id = auth.uid()
        )
    );

create policy "access_codes: pack owner can insert"
    on public.access_codes for insert
    with check (
        pack_id in (
            select id from public.gym_packs where owner_id = auth.uid()
        )
    );

create policy "access_codes: redeemer can read own"
    on public.access_codes for select
    using (redeemed_by = auth.uid());
