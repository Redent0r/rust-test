use regorus;

fn main() -> anyhow::Result<()> {
    // Create an engine for evaluating Rego policies.
    let mut engine = regorus::Engine::new();

    let policy = String::from(
        r#"
       package example
       import rego.v1

       default allow := false

       allow if {
         print("data.allowed_actions = ", data.allowed_actions)
         input.action in data.allowed_actions["user1"]
         print("This rule should be allowed")
       }
	"#,
    );

    // Add policy to the engine.
    engine.add_policy(String::from("policy.rego"), policy)?;

    // Evaluate first input. Expect to evaluate to false, since state is not set
    engine.set_input(regorus::Value::from_json_str(
        r#"{
      "action": "write"
    }"#,
    )?);

    let r = engine.eval_bool_query(String::from("data.example.allow"), false)?;
    println!("Received result: {:?}", r);
    assert_eq!(r, false);

    // Add data to engine. Set state
    engine.add_data(regorus::Value::from_json_str(
        r#"{
     "allowed_actions": {
        "user1" : ["read", "write"]
     }}"#,
    )?)?;

    // Evaluate second input. Expect to evaluate to true, since state has been set now
    engine.set_input(regorus::Value::from_json_str(
        r#"{
      "action": "write"
    }"#,
    )?);

    let r = engine.eval_bool_query(String::from("data.example.allow"), false)?;
    println!("Received result: {:?}", r);
    assert_eq!(
        r, true,
        "expect result to be true since rule evaluates to true after state has been updated, per rego logs"
    );

    Ok(())
}
