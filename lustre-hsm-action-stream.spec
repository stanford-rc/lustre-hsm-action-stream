%global project_name lustre-hsm-action-stream

Name:           python-%{project_name}
Version:        0.1.0
Release:        1%{?dist}
Summary:        A toolkit for shipping and reconciling Lustre HSM action events

License:        GPL-3.0-or-later
URL:            https://github.com/stanford-rc/%{project_name}
Source0:        %{project_name}-%{version}.tar.gz

BuildArch:      noarch

# BuildRequires are now handled by the new section below.
# We only need the core tools.
BuildRequires:  python3-devel
BuildRequires:  pyproject-rpm-macros

Requires:       python3-redis
Requires:       python3-PyYAML
Requires:       systemd-rpm-macros

%description
This package provides a lightweight toolkit for shipping Lustre HSM events
from MDT hsm/actions logs to a Redis stream.

# --- NEW SECTION ---
# This modern section dynamically generates BuildRequires from pyproject.toml
%generate_buildrequires
%pyproject_buildrequires

%prep
%autosetup -p1 -n %{project_name}-%{version}

%build
# --- REPLACED ---
# Use the modern macro to build a Python wheel.
%pyproject_wheel

%install
# This macro correctly installs the wheel built in the previous step.
%pyproject_install

# --- Manually adjust install locations for system binaries ---
install -d -m 755 %{buildroot}%{_sbindir}
mv %{buildroot}%{_bindir}/hsm-action-shipper %{buildroot}%{_sbindir}/
mv %{buildroot}%{_bindir}/hsm-stream-reconciler %{buildroot}%{_sbindir}/
# hsm-stream-janitor stays in /usr/bin

# --- Manual installation of non-Python files ---
install -d -m 755 %{buildroot}%{_sysconfdir}/%{project_name}
install -p -m 644 config/hsm_action_shipper.yaml %{buildroot}%{_sysconfdir}/%{project_name}/
install -p -m 644 config/hsm_stream_janitor.yaml %{buildroot}%{_sysconfdir}/%{project_name}/

install -d -m 755 %{buildroot}%{_unitdir}
install -p -m 644 systemd/hsm-action-shipper.service %{buildroot}%{_unitdir}/
install -p -m 644 systemd/hsm-stream-janitor.service %{buildroot}%{_unitdir}/

install -d -m 750 %{buildroot}/var/cache/hsm-action-shipper

%post
%systemd_post hsm-action-shipper.service
%systemd_post hsm-stream-janitor.service

%preun
%systemd_preun hsm-action-shipper.service
%systemd_preun hsm-stream-janitor.service

%postun
%systemd_postun_with_restart hsm-action-shipper.service
%systemd_postun_with_restart hsm-stream-janitor.service

%files
%license LICENSE
%doc README.md

# Configuration files
%config(noreplace) %{_sysconfdir}/%{project_name}/hsm_action_shipper.yaml
%config(noreplace) %{_sysconfdir}/%{project_name}/hsm_stream_janitor.yaml

# System binaries in /usr/sbin
%{_sbindir}/hsm-action-shipper
%{_sbindir}/hsm-stream-reconciler

# User binaries in /usr/bin
%{_bindir}/hsm-action-top
%{_bindir}/hsm-stream-janitor

# The Python library itself
%{python3_sitelib}/lustre_hsm_action_stream/

# --- UPDATED ---
# Building a wheel creates a .dist-info directory, not .egg-info.
%{python3_sitelib}/lustre_hsm_action_stream-%{version}.dist-info/

# Systemd service files
%{_unitdir}/hsm-action-shipper.service
%{_unitdir}/hsm-stream-janitor.service

# Own the cache directory
%dir /var/cache/hsm-action-shipper

%changelog
* Mon Sep 22 2025 Stephane Thiell <sthiell@stanford.edu> - 0.1.0-1
- Initial packaging
