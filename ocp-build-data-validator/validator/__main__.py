import argparse
import atexit
import sys
from multiprocessing import Pool, cpu_count

from . import format, support, schema, github, distgit, cgit
from . import exceptions, global_session


def validate(file, exclude_vpn, schema_only):
    (parsed, err) = format.validate(open(file).read())
    if err:
        msg = '{} is not a valid YAML\nReturned error: {}'.format(file, err)
        support.fail_validation(msg, parsed)

    if support.is_disabled(parsed):
        print('Skipping validation of disabled {}.'.format(file))
        return

    err = schema.validate(file, parsed)
    if err:
        msg = 'Schema mismatch: {}\nReturned error: {}'.format(file, err)
        support.fail_validation(msg, parsed)

    if support.get_artifact_type(file) not in ['image', 'rpm']:
        print(f'✅ Validated {file}')
        return

    if schema_only:
        print(f'✅ Validated {file}')
        return

    group_cfg = support.load_group_config_for(file)

    (url, err) = github.validate(parsed, group_cfg)
    if err:
        msg = ('GitHub validation failed for {} ({})\nReturned error: {}').format(file, url, err)
        support.fail_validation(msg, parsed)

    if exclude_vpn:
        print('Skipping distgit and cgit validations')
    else:
        (url, err) = cgit.validate(file, parsed, group_cfg)
        if err:
            msg = ('CGit validation failed for {} ({})\nReturned error: {}').format(file, url, err)
            support.fail_validation(msg, parsed)

            (url, err) = distgit.validate(file, parsed, group_cfg)
            if err:
                msg = ('DistGit validation failed for {} ({})\nReturned error: {}').format(file, url, err)
                support.fail_validation(msg, parsed)

    print(f'✅ Validated {file}')


def main():
    parser = argparse.ArgumentParser(description='Validation of ocp-build-data Image & RPM declarations')
    parser.add_argument('files', metavar='FILE', type=str, nargs='+', help='Files to be validated')
    parser.add_argument(
        '-s',
        '--single-thread',
        dest='single_thread',
        default=False,
        action='store_true',
        help='Run in single thread, so code.interact() works',
    )
    parser.add_argument(
        '--exclude-vpn',
        dest='exclude_vpn',
        default=False,
        action='store_true',
        help='Exclude validations that require vpn access',
    )
    parser.add_argument(
        '--schema-only', dest='schema_only', default=False, action='store_true', help='Only run schema validations'
    )
    args = parser.parse_args()
    print(f"Validating {len(args.files)} file(s)...")
    if args.single_thread:
        for f in args.files:
            validate(f, args.exclude_vpn, args.schema_only)
    else:
        try:
            rc = 0
            pool = Pool(cpu_count(), initializer=global_session.set_global_session)
            atexit.register(pool.close)
            pool.starmap(validate, [(f, args.exclude_vpn, args.schema_only) for f in args.files])
        except exceptions.ValidationFailedWIP as e:
            print(str(e), file=sys.stderr)
        except (exceptions.ValidationFailed, Exception) as e:
            print(str(e), file=sys.stderr)
            rc = 1

        finally:
            exit(rc)


if __name__ == "__main__":
    main()
