use std::collections::HashMap;
use std::path::PathBuf;

use nu_protocol::{IoStream, NuString, Span, Spanned};

use crate::ExternalCommand;

pub(crate) fn gen_command(
    span: Span,
    config_path: PathBuf,
    item: NuString,
    config_args: Vec<NuString>,
    env_vars_str: HashMap<NuString, NuString>,
) -> ExternalCommand {
    let name = Spanned { item, span };

    let mut args = vec![Spanned {
        item: config_path.to_string_lossy().into(),
        span: Span::unknown(),
    }];

    let number_of_args = config_args.len() + 1;

    for arg in config_args {
        args.push(Spanned {
            item: arg,
            span: Span::unknown(),
        })
    }

    ExternalCommand {
        name,
        args,
        arg_keep_raw: vec![false; number_of_args],
        out: IoStream::Inherit,
        err: IoStream::Inherit,
        env_vars: env_vars_str,
    }
}
