use nu_plugin::{EngineInterface, EvaluatedCall, LabeledError};
use nu_protocol::{ListStream, PipelineData, RawStream, Value};

pub struct Example;

mod int_or_float;
use self::int_or_float::IntOrFloat;

impl Example {
    pub fn seq(
        &self,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let first: i64 = call.req(0)?;
        let last: i64 = call.req(1)?;
        let span = call.head;
        let iter = (first..=last).map(move |number| Value::int(number, span));
        let list_stream = ListStream::from_stream(iter, None);
        Ok(PipelineData::ListStream(list_stream, None))
    }

    pub fn sum(
        &self,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let mut acc = IntOrFloat::Int(0);
        let span = input.span();
        for value in input {
            if let Ok(n) = value.as_i64() {
                acc.add_i64(n);
            } else if let Ok(n) = value.as_f64() {
                acc.add_f64(n);
            } else {
                return Err(LabeledError {
                    label: "Stream only accepts ints and floats".into(),
                    msg: format!("found {}", value.get_type()),
                    span,
                });
            }
        }
        Ok(PipelineData::Value(acc.to_value(call.head), None))
    }

    pub fn collect_external(
        &self,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let stream = input
            .into_iter()
            .map(|value| value.as_binary().map(|bin| bin.to_vec()));
        Ok(PipelineData::ExternalStream {
            stdout: Some(RawStream::new(Box::new(stream), None, call.head, None)),
            stderr: None,
            exit_code: None,
            span: call.head,
            metadata: None,
            trim_end_newline: false,
        })
    }

    pub fn for_each(
        &self,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let closure = call.req(0)?;
        let config = engine.get_config()?;
        for value in input {
            let result = engine.eval_closure(&closure, vec![value.clone()], Some(value))?;
            eprintln!("{}", result.into_string(", ", &config));
        }
        Ok(PipelineData::Empty)
    }
}
