# coding=utf-8

if __name__ == '__main__':
    # parser = OptionParser(usage="%prog [options]", version="%prog " + version)
    #
    # group = OptionGroup(parser, "Dangerous Options",
    #                     "Caution: use these options at your own risk.  "
    #                     "It is believed that some of them bite.")
    # group.add_option("-a", dest="a", help="Group option.")
    # parser.add_options(group)
    #
    # group = OptionGroup(parser, "Debug Options")
    # group.add_option("-d", "--debug", action="store_true",
    #                  help="Print debug information")
    # group.add_option("-s", "--sql", dest="sql",
    #                  help="Print all SQL statements executed")
    # group.add_option("-e", action="store_true", help="Print every action done")
    # parser.add_option_group(group)
    #
    # (options, args) = parser.parse_args()
    # print(options.sql)
    # print(len(args))
    # if len(args) != 1:
    #     parser.error("incorrect number of arguments")
    # if options.verbose:
    #     print "reading %s..." % options.filename

    # parser = OptionParser()
    # parser.add_option("-f", "--file", dest="filename",
    #                   help="write report to FILE", metavar="FILE")
    # parser.add_option("-q", "--quiet",
    #                   action="store_false", dest="verbose", default=True,
    #                   help="don't print status messages to stdout")
    #
    # (options, args) = parser.parse_args()

    import argparse

    parser = argparse.ArgumentParser(prefix_chars='--')

    subparsers = parser.add_subparsers(help='commands')

    # A list command
    list_parser = subparsers.add_parser('list', help='List contents')
    # list_parser.add_argument('dirname', action='store', help='Directory to list')

    # a = list_parser.add_subparsers(help='list')
    # b = a.add_parser("list-1")
    # b.add_argument("aa", action='store', dist="1")
    # group1 = list_parser.add_argument_group('group1', 'group1 description')
    # group1.add_argument('--foo', help='foo help')
    # group2 = list_parser.add_argument_group('group2', 'group1 description')
    # group2.add_argument('--foo', help='foo help')

    # A create command
    create_parser = subparsers.add_parser('create', help='Create a directory')
    create_parser.add_argument('--dirname', action='store', help='New directory to create')
    create_parser.add_argument('--read-only', default=False, action='store_true',
                               help='Set permissions to prevent writing to the directory')

    # A delete command
    delete_parser = subparsers.add_parser('delete', help='Remove a directory')
    delete_parser.add_argument('--dirname', action='store', help='The directory to remove')
    delete_parser.add_argument('--recursive', '-r', default=False, action='store_true',
                               help='Remove the contents of the directory, too')
    parser.get_default("list")
    print subparsers
    # print parser.parse_args().foo
    print parser.parse_known_args()
