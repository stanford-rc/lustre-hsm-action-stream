%global project_name lustre-hsm-action-stream

Name:           python-%{project_name}
Version:        0.2.0
Release:        1%{?dist}
Summary:        A toolkit for shipping Lustre HSM action events to Redis streams

License:        GPL-3.0-or-later
URL:            https://github.com/stanford-rc/%{project_name}
Source0:        %{project_name}-%{version}.tar.gz

BuildArch:      noarch

BuildRequires:  python3-devel
BuildRequires:  pyproject-rpm-macros

Requires:       python3-redis
Requires:       python3-PyYAML
Requires:       systemd-rpm-macros

%description
This package provides a lightweight, self-healing toolkit for shipping Lustre
HSM events from MDT hsm/actions logs to dedicated, per-MDT Redis streams.

# This modern section dynamically generates BuildRequires from pyproject.toml
%generate_buildrequires
%pyproject_buildrequires

%prep
%autosetup -p1 -n %{project_name}-%{version}

%build
# Use the modern macro to build a Python wheel.
%pyproject_wheel

%install
# This macro correctly installs the wheel built in the previous step.
%pyproject_install

# --- Manually adjust install locations for system binaries ---
install -d -m 755 %{buildroot}%{_sbindir}
# The shipper and reconciler are considered system-level tools.
mv %{buildroot}%{_bindir}/hsm-action-shipper %{buildroot}%{_sbindir}/
mv %{buildroot}%{_bindir}/hsm-stream-reconciler %{buildroot}%{_sbindir}/

# --- Manual installation of non-Python files ---
install -d -m 755 %{buildroot}%{_sysconfdir}/%{project_name}
install -p -m 644 config/hsm_action_shipper.yaml %{buildroot}%{_sysconfdir}/%{project_name}/
install -p -m 644 config/hsm_stream_stats.yaml %{buildroot}%{_sysconfdir}/%{project_name}/
install -p -m 644 config/hsm_stream_tail.yaml %{buildroot}%{_sysconfdir}/%{project_name}/

install -d -m 755 %{buildroot}%{_unitdir}
install -p -m 644 systemd/hsm-action-shipper.service %{buildroot}%{_unitdir}/

install -d -m 750 %{buildroot}/var/cache/hsm-action-shipper

# --- Systemd Service Lifecycle ---
%post
%systemd_post hsm-action-shipper.service
# REMOVED: Janitor service macros are obsolete.

%preun
%systemd_preun hsm-action-shipper.service
# REMOVED: Janitor service macros are obsolete.

%postun
%systemd_postun_with_restart hsm-action-shipper.service
# REMOVED: Janitor service macros are obsolete.

%files
%license LICENSE
%doc README.md

# --- Configuration files ---
%config(noreplace) %{_sysconfdir}/%{project_name}/hsm_action_shipper.yaml
%config(noreplace) %{_sysconfdir}/%{project_name}/hsm_stream_stats.yaml
%config(noreplace) %{_sysconfdir}/%{project_name}/hsm_stream_tail.yaml

# --- Binaries ---
# System binaries in /usr/sbin
%{_sbindir}/hsm-action-shipper
%{_sbindir}/hsm-stream-reconciler

# User binaries in /usr/bin
%{_bindir}/hsm-action-top
%{_bindir}/hsm-stream-stats
%{_bindir}/hsm-stream-tail

# --- Python Library and Metadata ---
%{python3_sitelib}/lustre_hsm_action_stream/
%{python3_sitelib}/lustre_hsm_action_stream-%{version}.dist-info/

# --- Systemd Service ---
%{_unitdir}/hsm-action-shipper.service

# --- Directories Owned by this Package ---
%dir /var/cache/hsm-action-shipper

%changelog
* Wed Oct  1 2025 Stephane Thiell <sthiell@stanford.edu> - 0.2.0-1
- Replaced global stream with per-MDT streams.
- Removed hsm-stream-janitor daemon.
- Integrated self-healing reconciliation into hsm-action-shipper.
- Added a public consumer API (StreamReader).
- Overhauled test suite to use pytest.

* Mon Sep 22 2025 Stephane Thiell <sthiell@stanford.edu> - 0.1.0-1
- Initial packaging
